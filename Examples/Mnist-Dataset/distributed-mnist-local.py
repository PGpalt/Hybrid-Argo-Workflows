#!/usr/bin/env python3

from __future__ import print_function

import argparse
import logging
import os
import socket
import subprocess
import time
import re
import torch
import torch.distributed as dist
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms
import torchvision.datasets.utils as utils

def find_free_port():
    """Finds a free port on localhost."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((os.environ["MASTER_ADDR"], 0))
    port = s.getsockname()[1]
    s.close()
    return port

def set_master_addr():
    # If MASTER_ADDR is already set, do nothing.
    if "MASTER_ADDR" in os.environ:
        print("MASTER_ADDR already set to:", os.environ["MASTER_ADDR"])
        return

    # Retrieve SLURM_NODELIST from environment.
    slurm_nodelist = os.environ.get("SLURM_NODELIST")
    if not slurm_nodelist:
        raise RuntimeError("MASTER_ADDR not set and SLURM_NODELIST not found.")

    try:
        # Get the first node from SLURM_NODELIST (it may be a comma-separated list).
        first_node = slurm_nodelist.split(",")[0]
        # Query detailed information for the node.
        node_info = subprocess.check_output(["scontrol", "show", "node", "-o", first_node]).decode()
        print("Node info:", node_info)
        # Extract the NodeAddr field.
        m = re.search(r"NodeAddr=([\w\.]+)", node_info)
        if m:
            master_ip = m.group(1)
        else:
            raise RuntimeError("Could not extract NodeAddr from node info.")
        os.environ["MASTER_ADDR"] = master_ip
        print("MASTER_ADDR set to:", master_ip)
    except Exception as e:
        raise RuntimeError("Could not determine MASTER_ADDR using NodeAddr") from e

def setup_master_port():
    """
    If rank 0, find a free port and write it to a temporary file.
    If not rank 0, wait until the file is available, then read the port.
    """
    # Create a unique filename based on SLURM_JOB_ID if available, otherwise use the PID.
    job_id = os.environ.get("SLURM_JOB_ID", str(os.getpid()))
    port_file = f"/tmp/master_port_{job_id}.txt"
    
    if RANK == 0:
        # Master finds a free port.
        free_port = find_free_port()
        os.environ["MASTER_PORT"] = str(free_port)
        print("MASTER_PORT (master) dynamically set to:", free_port)
        # Write the free port to the file.
        with open(port_file, "w") as f:
            f.write(str(free_port))
    else:
        # Worker: Wait until the master writes the free port.
        timeout = 60  # seconds
        start_time = time.time()
        while not os.path.exists(port_file):
            if time.time() - start_time > timeout:
                raise RuntimeError("Timeout waiting for master port file.")
            time.sleep(1)
        with open(port_file, "r") as f:
            free_port = f.read().strip()
        os.environ["MASTER_PORT"] = free_port
        print("MASTER_PORT (worker) read as:", free_port)
    return port_file

# Set WORLD_SIZE and RANK from SLURM environment variables if available.
if "SLURM_NTASKS" in os.environ:
    os.environ["WORLD_SIZE"] = os.environ["SLURM_NTASKS"]
if "SLURM_PROCID" in os.environ:
    os.environ["RANK"] = os.environ["SLURM_PROCID"]

WORLD_SIZE = int(os.environ.get("WORLD_SIZE", 1))
RANK = int(os.environ.get("RANK", 0))

# Set up MASTER_ADDR and MASTER_PORT.
set_master_addr()
port_file = setup_master_port()



class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 20, 5, 1)
        self.conv2 = nn.Conv2d(20, 50, 5, 1)
        self.fc1 = nn.Linear(4 * 4 * 50, 500)
        self.fc2 = nn.Linear(500, 10)

    def forward(self, x):
        x = F.relu(self.conv1(x))
        x = F.max_pool2d(x, 2, 2)
        x = F.relu(self.conv2(x))
        x = F.max_pool2d(x, 2, 2)
        x = x.view(-1, 4 * 4 * 50)
        x = F.relu(self.fc1(x))
        x = self.fc2(x)
        return F.log_softmax(x, dim=1)


def train(args, model, device, train_loader, optimizer, epoch, train_sampler=None):
    model.train()
    if train_sampler is not None:
        train_sampler.set_epoch(epoch)
    total_loss = 0.0
    total_samples = 0

    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        # F.nll_loss uses reduction="mean" by default.
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()

        # Multiply by batch size to get the sum loss for this batch.
        batch_size = data.size(0)
        total_loss += loss.item() * batch_size
        total_samples += batch_size

        if batch_idx % args.log_interval == 0:
            msg = "Train Epoch: {} [{}/{} ({:.0f}%)]\tloss={:.4f}".format(
                epoch,
                batch_idx * len(data),
                len(train_loader.dataset),
                100.0 * batch_idx / len(train_loader),
                loss.item(),
            )
            logging.info(msg)
            # logging.info("{{metricName: loss, metricValue: {:.4f}}}".format(loss.item()))


def test(args, model, device, test_loader, epoch, test_sampler=None):
    model.eval()
    total_loss = 0.0
    total_correct = 0
    total_samples = 0

    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            # Use sum reduction to aggregate loss over the batch.
            batch_loss = F.nll_loss(output, target, reduction="sum").item()
            total_loss += batch_loss
            pred = output.max(1, keepdim=True)[1]
            total_correct += pred.eq(target.view_as(pred)).sum().item()
            total_samples += data.size(0)

    if is_distributed():
        loss_tensor = torch.tensor(total_loss, device=device)
        correct_tensor = torch.tensor(total_correct, device=device)
        samples_tensor = torch.tensor(total_samples, device=device)
        dist.all_reduce(loss_tensor, op=dist.ReduceOp.SUM)
        dist.all_reduce(correct_tensor, op=dist.ReduceOp.SUM)
        dist.all_reduce(samples_tensor, op=dist.ReduceOp.SUM)
        aggregated_loss = loss_tensor.item() / samples_tensor.item()
        aggregated_accuracy = correct_tensor.item() / samples_tensor.item()
    else:
        aggregated_loss = total_loss / total_samples
        aggregated_accuracy = total_correct / total_samples

    if RANK == 0:
        # Preserve the original metric logging format.
        logging.info(
            "{{metricName: loss, metricValue: {:.4f}}}\n".format(
                aggregated_loss
            )
        )


def should_distribute():
    return dist.is_available() and WORLD_SIZE > 1


def is_distributed():
    return dist.is_available() and dist.is_initialized()


def main():
    # Training settings
    parser = argparse.ArgumentParser(description="PyTorch MNIST Example")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=64,
        metavar="N",
        help="input batch size for training (default: 64)",
    )
    parser.add_argument(
        "--test-batch-size",
        type=int,
        default=1000,
        metavar="N",
        help="input batch size for testing (default: 1000)",
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=10,
        metavar="N",
        help="number of epochs to train (default: 10)",
    )
    parser.add_argument(
        "--lr",
        type=float,
        default=0.01,
        metavar="LR",
        help="learning rate (default: 0.01)",
    )
    parser.add_argument(
        "--momentum",
        type=float,
        default=0.5,
        metavar="M",
        help="SGD momentum (default: 0.5)",
    )
    parser.add_argument(
        "--no-cuda", action="store_true", default=False, help="disables CUDA training"
    )
    parser.add_argument(
        "--seed", type=int, default=1, metavar="S", help="random seed (default: 1)"
    )
    parser.add_argument(
        "--log-interval",
        type=int,
        default=100,
        metavar="N",
        help="how many batches to wait before logging training status",
    )
    parser.add_argument(
        "--log-path",
        type=str,
        default="",
        help="Path to save logs. Print to StdOut if log-path is not set",
    )
    parser.add_argument(
        "--save-model",
        action="store_true",
        default=False,
        help="For Saving the current Model",
    )
    parser.add_argument(
        "--logger",
        type=str,
        choices=["standard"],
        help="Logger",
        default="standard",
    )
    parser.add_argument(
        "--resume", 
        action="store_true", 
        default=False,
        help="Resume training from checkpoint"
    )

    if dist.is_available():
        parser.add_argument(
            "--backend",
            type=str,
            help="Distributed backend",
            choices=[dist.Backend.GLOO, dist.Backend.NCCL, dist.Backend.MPI],
            default=dist.Backend.GLOO,
        )
    args = parser.parse_args()

    # Set up logging: if log_path is empty, print to StdOut.
    if args.log_path == "":
        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
            level=logging.DEBUG,
        )
    else:
        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
            level=logging.DEBUG,
            filename=args.log_path,
        )

    use_cuda = not args.no_cuda and torch.cuda.is_available()
    print("CUDA AVAILABILITY: ", torch.cuda.is_available())
    if use_cuda:
        print("Using CUDA")
    torch.manual_seed(args.seed)
    device = torch.device("cuda" if use_cuda else "cpu")

    if should_distribute():
        print("Using distributed PyTorch with {} backend".format(args.backend))
        if not dist.is_initialized():
            print("Initializing process group...")
            dist.init_process_group(backend=args.backend)

    kwargs = {"num_workers": 1, "pin_memory": True} if use_cuda else {}

    if RANK == 0:
        train_dataset = datasets.FashionMNIST(
            "./data",
            train=True,
            download=True,
            transform=transforms.Compose([transforms.ToTensor()]),
        )
        test_dataset = datasets.FashionMNIST(
            "./data",
            train=False,
            transform=transforms.Compose([transforms.ToTensor()]),
        )
        if is_distributed():
            dist.barrier()  # let other processes wait until download is complete
    else:
        if is_distributed():
            dist.barrier()  # wait for rank 0 to finish downloading
        train_dataset = datasets.FashionMNIST(
            "./data",
            train=True,
            download=False,
            transform=transforms.Compose([transforms.ToTensor()]),
        )
        test_dataset = datasets.FashionMNIST(
            "./data",
            train=False,
            download=False,
            transform=transforms.Compose([transforms.ToTensor()]),
        )


    # Set up DistributedSampler if in distributed mode.
    if is_distributed():
        train_sampler = torch.utils.data.distributed.DistributedSampler(train_dataset)
        # For testing, you might also use a DistributedSampler. Note that if you do,
        # each process will only see a portion of the test dataset.
        test_sampler = torch.utils.data.distributed.DistributedSampler(test_dataset, shuffle=False)
    else:
        train_sampler = None
        test_sampler = None

    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=args.batch_size,
        sampler=train_sampler,
        shuffle=(train_sampler is None),
        **kwargs,
    )

    test_loader = torch.utils.data.DataLoader(
        test_dataset,
        batch_size=args.test_batch_size,
        sampler=test_sampler,
        shuffle=False,
        **kwargs,
    )

    model = Net().to(device)

    if is_distributed():
        Distributor = (
            nn.parallel.DistributedDataParallel
            if use_cuda
            else nn.parallel.DistributedDataParallelCPU
        )
        model = Distributor(model)

    optimizer = optim.SGD(model.parameters(), lr=args.lr, momentum=args.momentum)

     # Resume training from checkpoint if flag is set
    checkpoint_path = "mnist_cnn.pt"
    if args.resume and os.path.isfile(checkpoint_path):
        checkpoint = torch.load(checkpoint_path, map_location=device)
        model.load_state_dict(checkpoint)
        # Optionally, if you saved the optimizer state and epoch, load them too:
        # optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        # start_epoch = checkpoint['epoch'] + 1
        print("Checkpoint loaded, resuming training.")

    for epoch in range(1, args.epochs + 1):
        train(args, model, device, train_loader, optimizer, epoch, train_sampler)
        test(args, model, device, test_loader, epoch)

    if args.save_model:
        torch.save(model.state_dict(), "mnist_cnn.pt")


if __name__ == "__main__":
    main()

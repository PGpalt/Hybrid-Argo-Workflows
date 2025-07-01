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

import numpy as np

# --- Custom Dataset: Raw FashionMNIST from numpy ---
class FashionMNIST_Numpy(torch.utils.data.Dataset):
    def __init__(self, images_path, labels_path, transform=None):
        self.images = self._read_images(images_path)
        self.labels = self._read_labels(labels_path)
        self.transform = transform

    def _read_images(self, path):
        with open(path, 'rb') as f:
            data = np.frombuffer(f.read(), np.uint8, offset=16)
        images = data.reshape(-1, 28, 28)
        return images

    def _read_labels(self, path):
        with open(path, 'rb') as f:
            data = np.frombuffer(f.read(), np.uint8, offset=8)
        return data

    def __getitem__(self, idx):
        img = self.images[idx].astype(np.float32) / 255.0  # scale to [0,1]
        img = torch.from_numpy(img).unsqueeze(0)  # add channel: (1,28,28)
        label = int(self.labels[idx])
        if self.transform:
            img = self.transform(img)
        return img, label

    def __len__(self):
        return len(self.images)

# --- Distributed setup and helpers (as in your script) ---
def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((os.environ["MASTER_ADDR"], 0))
    port = s.getsockname()[1]
    s.close()
    return port

def set_master_addr():
    if "MASTER_ADDR" in os.environ:
        print("MASTER_ADDR already set to:", os.environ["MASTER_ADDR"])
        return
    slurm_nodelist = os.environ.get("SLURM_NODELIST")
    if not slurm_nodelist:
        raise RuntimeError("MASTER_ADDR not set and SLURM_NODELIST not found.")
    try:
        first_node = slurm_nodelist.split(",")[0]
        node_info = subprocess.check_output(["scontrol", "show", "node", "-o", first_node]).decode()
        print("Node info:", node_info)
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
    workdir = os.environ.get("SLURM_SUBMIT_DIR", "/tmp")
    job_id = os.environ.get("SLURM_JOB_ID", str(os.getpid()))
    port_file = os.path.join(workdir, f"master_port_{job_id}.txt")
    if RANK == 0:
        free_port = find_free_port()
        os.environ["MASTER_PORT"] = str(free_port)
        print("MASTER_PORT (master) dynamically set to:", free_port)
        with open(port_file, "w") as f:
            f.write(str(free_port))
    else:
        timeout = 60
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

if "SLURM_NTASKS" in os.environ:
    os.environ["WORLD_SIZE"] = os.environ["SLURM_NTASKS"]
if "SLURM_PROCID" in os.environ:
    os.environ["RANK"] = os.environ["SLURM_PROCID"]

WORLD_SIZE = int(os.environ.get("WORLD_SIZE", 1))
RANK = int(os.environ.get("RANK", 0))

set_master_addr()
port_file = setup_master_port()
# if is_distributed():
#     dist.barrier()  # make sure all workers have initialized
# if RANK == 0 and os.path.exists(port_file):
#     os.remove(port_file)

# --- Model ---
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

# --- Training/testing ---
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
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
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

def test(args, model, device, test_loader, epoch, test_sampler=None):
    model.eval()
    total_loss = 0.0
    total_correct = 0
    total_samples = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
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
        logging.info(
            "{{metricName: loss, metricValue: {:.4f}}}\n".format(
                aggregated_loss
            )
        )

def should_distribute():
    return dist.is_available() and WORLD_SIZE > 1

def is_distributed():
    return dist.is_available() and dist.is_initialized()

# --- MAIN ---
def main():
    parser = argparse.ArgumentParser(description="PyTorch FashionMNIST Example (No torchvision)")
    parser.add_argument("--batch-size", type=int, default=64, metavar="N")
    parser.add_argument("--test-batch-size", type=int, default=1000, metavar="N")
    parser.add_argument("--epochs", type=int, default=10, metavar="N")
    parser.add_argument("--lr", type=float, default=0.01, metavar="LR")
    parser.add_argument("--momentum", type=float, default=0.5, metavar="M")
    parser.add_argument("--no-cuda", action="store_true", default=False)
    parser.add_argument("--seed", type=int, default=1, metavar="S")
    parser.add_argument("--log-interval", type=int, default=100, metavar="N")
    parser.add_argument("--log-path", type=str, default="")
    parser.add_argument("--save-model", action="store_true", default=False)
    parser.add_argument("--logger", type=str, choices=["standard", "hypertune"], default="standard")
    parser.add_argument("--resume", action="store_true", default=False)
    if dist.is_available():
        parser.add_argument(
            "--backend",
            type=str,
            help="Distributed backend",
            choices=[dist.Backend.GLOO, dist.Backend.NCCL, dist.Backend.MPI],
            default=dist.Backend.GLOO,
        )
    args = parser.parse_args()

    if args.log_path == "" or args.logger == "hypertune":
        logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ", level=logging.DEBUG)
    else:
        logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ", level=logging.DEBUG, filename=args.log_path)

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

    # ---- DATASET (NO torchvision!) ----
    data_dir = "./data"
    train_images = os.path.join(data_dir, "train-images-idx3-ubyte")
    train_labels = os.path.join(data_dir, "train-labels-idx1-ubyte")
    test_images = os.path.join(data_dir, "t10k-images-idx3-ubyte")
    test_labels = os.path.join(data_dir, "t10k-labels-idx1-ubyte")

    if RANK == 0:
        train_dataset = FashionMNIST_Numpy(train_images, train_labels)
        test_dataset = FashionMNIST_Numpy(test_images, test_labels)
        if is_distributed():
            dist.barrier()
    else:
        if is_distributed():
            dist.barrier()
        train_dataset = FashionMNIST_Numpy(train_images, train_labels)
        test_dataset = FashionMNIST_Numpy(test_images, test_labels)

    if is_distributed():
        train_sampler = torch.utils.data.distributed.DistributedSampler(train_dataset)
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

    checkpoint_path = "mnist_cnn.pt"
    if args.resume and os.path.isfile(checkpoint_path):
        checkpoint = torch.load(checkpoint_path, map_location=device)
        model.load_state_dict(checkpoint)
        print("Checkpoint loaded, resuming training.")

    for epoch in range(1, args.epochs + 1):
        train(args, model, device, train_loader, optimizer, epoch, train_sampler)
        test(args, model, device, test_loader, epoch)

    if args.save_model:
        torch.save(model.state_dict(), "mnist_cnn.pt")

if __name__ == "__main__":
    main()

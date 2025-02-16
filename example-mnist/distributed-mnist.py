#!/usr/bin/env python3
# Copyright 2022 The Kubeflow Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import argparse
import logging
import os

import hypertune
import torch
import torch.distributed as dist
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms

WORLD_SIZE = int(os.environ.get("WORLD_SIZE", 1))


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

    # Compute the average training loss for this epoch.
    local_avg_loss = total_loss / total_samples if total_samples > 0 else 0.0

    # If distributed, aggregate total loss and total samples from all processes.
    if is_distributed():
        loss_tensor = torch.tensor(total_loss, device=device)
        samples_tensor = torch.tensor(total_samples, device=device)
        dist.all_reduce(loss_tensor, op=dist.ReduceOp.SUM)
        dist.all_reduce(samples_tensor, op=dist.ReduceOp.SUM)
        aggregated_loss = loss_tensor.item() / samples_tensor.item()
    else:
        aggregated_loss = local_avg_loss

    # Log aggregated training loss only from the master process.
    rank = 0
    if is_distributed():
        rank = dist.get_rank()
    if rank == 0:
        # Logging in a similar JSON-like metric format.
        logging.info("{{metricName: train_loss, metricValue: {:.4f}}}\n".format(aggregated_loss))


def test(args, model, device, test_loader, epoch, hpt, test_sampler=None):
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

    rank = 0
    if is_distributed():
        rank = dist.get_rank()
    if rank == 0:
        # Preserve the original metric logging format.
        logging.info(
            "{{metricName: accuracy, metricValue: {:.4f}}};"
            "{{metricName: loss, metricValue: {:.4f}}}\n".format(
                aggregated_accuracy, aggregated_loss
            )
        )
        if args.logger == "hypertune":
            hpt.report_hyperparameter_tuning_metric(
                hyperparameter_metric_tag="loss",
                metric_value=aggregated_loss,
                global_step=epoch,
            )
            hpt.report_hyperparameter_tuning_metric(
                hyperparameter_metric_tag="accuracy",
                metric_value=aggregated_accuracy,
                global_step=epoch,
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
        default=10,
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
        choices=["standard", "hypertune"],
        help="Logger",
        default="standard",
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

    # Set up logging: if log_path is empty or using hypertune, print to StdOut.
    if args.log_path == "" or args.logger == "hypertune":
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

    if args.logger == "hypertune" and args.log_path != "":
        os.environ["CLOUD_ML_HP_METRIC_FILE"] = args.log_path

    # For JSON logging with hypertune.
    hpt = hypertune.HyperTune()

    use_cuda = not args.no_cuda and torch.cuda.is_available()
    if use_cuda:
        print("Using CUDA")
    torch.manual_seed(args.seed)
    device = torch.device("cuda" if use_cuda else "cpu")

    if should_distribute():
        print("Using distributed PyTorch with {} backend".format(args.backend))
        dist.init_process_group(backend=args.backend)
        logging.info("Distributed Enviroment")

    kwargs = {"num_workers": 1, "pin_memory": True} if use_cuda else {}

    # Only let rank 0 download the dataset and then synchronize with others.
    rank = 0
    if is_distributed():
        rank = dist.get_rank()

    if rank == 0:
        train_dataset = datasets.FashionMNIST(
            "./data",
            train=True,
            download=True,
            transform=transforms.Compose([transforms.ToTensor()]),
        )
        test_dataset = datasets.FashionMNIST(
            "./data",
            train=False,
            download=True,
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

    # Set up DistributedSampler if running in distributed mode.
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

    for epoch in range(1, args.epochs + 1):
        train(args, model, device, train_loader, optimizer, epoch, train_sampler)
        test(args, model, device, test_loader, epoch, hpt, test_sampler)

    if args.save_model:
        torch.save(model.state_dict(), "mnist_cnn.pt")


if __name__ == "__main__":
    main()

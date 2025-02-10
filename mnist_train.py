
#!/usr/bin/env python3
import argparse
import os
import torch
import torch.nn as nn
import torch.optim as optim
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from torchvision import datasets, transforms

# Define a simple CNN for MNIST
class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 10, kernel_size=5)
        self.conv2 = nn.Conv2d(10, 20, kernel_size=5)
        self.fc1 = nn.Linear(320, 50)
        self.fc2 = nn.Linear(50, 10)
    
    def forward(self, x):
        x = torch.relu(torch.max_pool2d(self.conv1(x), 2))
        x = torch.relu(torch.max_pool2d(self.conv2(x), 2))
        x = torch.flatten(x, 1)
        x = torch.relu(self.fc1(x))
        x = self.fc2(x)
        return torch.log_softmax(x, dim=1)

def setup_distributed():
    # If running in a distributed setting, initialize the process group.
    if 'RANK' in os.environ and 'WORLD_SIZE' in os.environ:
        rank = int(os.environ['RANK'])
        world_size = int(os.environ['WORLD_SIZE'])
        print(f"Initializing distributed training: rank {rank}/{world_size}")
        dist.init_process_group(backend="gloo", rank=rank, world_size=world_size)
        return True
    return False

def train(args):
    distributed = setup_distributed()
    device = torch.device("cpu")
    
    # Prepare the MNIST dataset and dataloader
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
    ])
    train_dataset = datasets.MNIST('./data', train=True, download=True, transform=transform)
    
    if distributed:
        train_sampler = torch.utils.data.distributed.DistributedSampler(train_dataset)
    else:
        train_sampler = None

    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=args.batch_size,
        shuffle=(train_sampler is None),
        sampler=train_sampler
    )
    
    # Create model, optimizer, and loss function
    model = Net().to(device)
    if distributed:
        model = DDP(model)
    optimizer = optim.SGD(model.parameters(), lr=args.lr)
    criterion = nn.NLLLoss()
    
    model.train()
    for epoch in range(args.epochs):
        if distributed:
            train_sampler.set_epoch(epoch)
        total_loss = 0.0
        for batch_idx, (data, target) in enumerate(train_loader):
            data, target = data.to(device), target.to(device)
            optimizer.zero_grad()
            output = model(data)
            loss = criterion(output, target)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
            if batch_idx % 100 == 0:
                print(f"Epoch {epoch} Batch {batch_idx} Loss {loss.item()}")
        print(f"Epoch {epoch} average loss: {total_loss / len(train_loader):.4f}")
    
    if distributed:
        dist.destroy_process_group()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--lr', type=float, default=0.01, help='learning rate')
    parser.add_argument('--batch_size', type=int, default=64, help='input batch size')
    parser.add_argument('--epochs', type=int, default=2, help='number of epochs')
    args = parser.parse_args()
    train(args)

if __name__ == "__main__":
    main()

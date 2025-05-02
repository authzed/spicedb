#!/bin/bash
# This script is executed after the creation of a new project.

# Install goreleaser pro
echo 'deb [trusted=yes] https://repo.goreleaser.com/apt/ /' | sudo tee /etc/apt/sources.list.d/goreleaser.list
sudo apt update
sudo apt install goreleaser-pro
# Installation scripts

> [!WARNING]
> These scripts are **outdated**, please use [Lifeblood-Manager](https://github.com/pedohorse/lifeblood-manager/releases)
> instead!


There are multiple way of installing lifeblood, you can easily do it with pip,
but here you can find even simpler "one-click" solutions

see [documentation](https://pedohorse.github.io/lifeblood/installation.html)

### Linux/MacOs (never tested on macos)

* `install.sh` - just copy install.sh to an empty directory where you want **Lifeblood**
  to be installed and run it from the terminal.
  
### Windows

* `install.ps1` - just copy install.ps1 to an empty directory where you want **Lifeblood**
  to be installed and run it from the terminal.

Unlike `pip` method, this will bring the freshest from github.  
By default it will bring the latest master branch, but you can specify another branch to it.  
You can also use the same script for updating lifeblood.
different lifeblood versions will be installed in different subdirectories,
with `current` (symlink) pointing at the latest installed
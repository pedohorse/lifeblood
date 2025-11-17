{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-25.05";
  };

  outputs = { self, nixpkgs }:
  let
    pkgs = nixpkgs.legacyPackages.x86_64-linux;
  in {

    packages.x86_64-linux = {
      lifeblood = pkgs.callPackage ./lifeblood.nix {};
      ass = pkgs.python311Packages.aiorwlock;
    };
  };
}

{
  description = "Lifeblood!";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-25.05";
  };

  outputs = { self, nixpkgs }:
  let
    pkgs = nixpkgs.legacyPackages.x86_64-linux;
  in {

    packages.x86_64-linux = rec {
      lifeblood = pkgs.callPackage ./lifeblood.nix {};
      lifeblood-viewer = pkgs.callPackage ./lifeblood-viewer.nix { inherit lifeblood; };
      tests = import ./lifeblood-integration-tests.nix { pkgs = nixpkgs.legacyPackages.x86_64-linux.extend (final: prev: { inherit lifeblood; }); };
    };
  };
}

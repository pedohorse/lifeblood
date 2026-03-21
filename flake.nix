{
  description = "Lifeblood!";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = { self, nixpkgs}:
  let
    pkgs = nixpkgs.legacyPackages.x86_64-linux;
  in {

    packages.x86_64-linux = rec {
      default = lifeblood;
      lifeblood = pkgs.python312Packages.callPackage ./lifeblood.nix { };
      lifeblood-viewer = pkgs.python312Packages.callPackage ./lifeblood-viewer.nix { inherit lifeblood; };
      tests = import ./lifeblood-integration-tests.nix { pkgs = nixpkgs.legacyPackages.x86_64-linux.extend (final: prev: { inherit lifeblood; }); };
    };
  };
}

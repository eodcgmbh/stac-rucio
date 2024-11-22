#  Using a specific tarball to ensure a consistent Poetry version.
let
  pkgs = import (builtins.fetchTarball {
      url = "https://github.com/NixOS/nixpkgs/archive/270dace49bc95a7f88ad187969179ff0d2ba20ed.tar.gz";
  }) {};
in

pkgs.mkShellNoCC {
  packages = with pkgs; [
    pkgs.poetry
    pkgs.git
  ];

  # Commands to execute when entering the shell
  shellHook = ''
    # Run any commands you want here
    poetry lock
    poetry install --all-extras
    poetry run pre-commit install
    echo "Poetry version: $(poetry --version)"
    echo "Git version: $(git --version)"
  '';
}

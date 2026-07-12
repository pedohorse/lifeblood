{
  lib,
  fetchPypi,
  buildPythonPackage,
  buildPythonApplication,
  autoPatchelfHook,
  libx11,
  libxext,
  libz,
  lifeblood,
  setuptools,
  glfw,
  pyopengl,
  pyside6,
  lz4,
  grandalf,
  numpy,
  qt6,
}:
let
  imgui_bundle = buildPythonPackage {
    name = "imgui_bundle";
    pythonImportsCheck = [ "imgui_bundle" ];
    build-system = [ setuptools ];
    format = "wheel";
    src = fetchPypi {
      pname = "imgui_bundle";
      version = "1.92.801";
      format = "wheel";
      dist = "cp313";
      python = "cp313";
      abi = "cp313";
      platform = "manylinux_2_28_x86_64";
      hash = "sha256-m2wEM0RGy9x7GHXO7LdiSFvEDoNBzRXFQGunA0alxrs=";
    };
    buildInputs = [
      libx11
      libz
      libxext
    ];
    nativeBuildInputs = [
      autoPatchelfHook
    ];
  };

in buildPythonApplication {
  name = "lifeblood-viewer";
  pyproject = true;
  src = ./.;

  patchPhase = ''
    cp ./pkg_lifeblood_viewer/{pyproject.toml,setup.cfg} .
  '';

  build-system = [ setuptools ];

  buildInputs = [
    qt6.qtbase
    qt6.qtwayland
  ];
  nativeBuildInputs = [
    qt6.wrapQtAppsHook
  ];
  dontWrapQtApps = true;
  makeWrapperArgs = [
    "\${qtWrapperArgs[@]}"
  ];

  pythonImportsCheck = [ "lifeblood" ];
  propagatedBuildInputs = [
    glfw
    pyopengl
    pyside6
    lz4
    grandalf
    numpy
  ] ++ [
    imgui_bundle
    lifeblood
  ];

  meta = {
    description = "";
    license = lib.licenses.gpl3;
    mainProgram = "lifeblood_viewer";
  };
}

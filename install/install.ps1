# =========================
# = Configuration Section =
# =========================
$branch = "dev"
$pyver = "3.10.9"
# ================================
# = End of Configuration Section =
# ================================

$pyver -match "(\d+)\.(\d+)"
$pyxy = $Matches.0
$pycode = $Matches.1 + $Matches.2
$install_viewer = $true

# parse args

foreach($arg in $args){
    if($arg -eq "--no-viewer"){
        $install_viewer = $false
    }elseif($arg -eq "--help" -or $arg -eq "-h"){
        Write-Host "install.ps1 usage:"
        Write-Host "    --no-viewer    do NOT install lifeblood_viewer. SHOULD be used for headless machines!"
        Write-Host "    -h / --help      shot this message"
        exit
    }
}

# download embedded python
$archname = "python-$pyver-embed-win32.zip"
Write-Host "downloading embedded python..."
(New-Object System.Net.WebClient).DownloadFile("https://www.python.org/ftp/python/$pyver/$archname", $archname)
if(Test-Path temp_python_bin){
    Remove-Item -Recurse temp_python_bin
}
Expand-Archive $archname -DestinationPath temp_python_bin
Remove-Item $archname

# download lifeblood
Write-Host "downloading latest $branch..."
(New-Object System.Net.WebClient).DownloadFile("https://github.com/pedohorse/lifeblood/archive/refs/heads/${branch}.zip", "$branch.zip")

(temp_python_bin/python -c 'import zipfile;print(zipfile.ZipFile(""dev.zip"", ""r"").comment.decode())') -match ".{13}"
$hash = $Matches.0
Write-Host "new version has hash: $hash"
if(Test-Path $hash){
    Write-Host "folder named $host already exists, aborting"
    exit 1
}

New-Item $hash -ItemType "directory"

Write-Host "moving python into new version"
Move-Item temp_python_bin -Destination $hash/bin
Write-Host "extracting lifeblood"
Expand-Archive "$branch.zip" -DestinationPath $hash
$lbsrcdir = "lifeblood-$branch"

Push-Location $hash

'include-system-site-packages = false' | Set-Content -Path pyvenv.cfg
'import site' | Add-Content -Path "bin/python$pycode._pth"

$sitepath = "lib/site-packages"
New-Item $sitepath -ItemType "directory"

Write-Host "downloading bootstrap pip..."
New-Item "piptmp" -ItemType "directory"
(New-Object System.Net.WebClient).DownloadFile("https://bootstrap.pypa.io/pip/pip.pyz", "$hash/piptmp/pip.pyz")  # why that path after pushd? no idea!

Write-Host "downloading dependencies from pypi"
(Get-Content "$lbsrcdir/pkg_lifeblood/setup.cfg " -Raw `
    | Select-String "(?sm)(?<=install_requires.*?\r?\n).*?(?=\r?\n\s*\r?\n)").Matches.Value `
    | Set-Content -Path  "piptmp/requirements.txt"
Start-Process -NoNewWindow -Wait -WorkingDirectory "piptmp" -FilePath "bin/python" -ArgumentList "pip.pyz install --target=../$sitepath --only-binary :all: --platform win32 --python-version $pyver -r requirements.txt"
if($install_viewer){
    (Get-Content "$lbsrcdir/pkg_lifeblood_viewer/setup.cfg " -Raw `
        | Select-String "(?sm)(?<=install_requires.*?\r?\n).*?(?=\r?\n\s*\r?\n)").Matches.Value `
         -replace "(?m)lifeblood.*$","" `
        | Set-Content -Path  "piptmp/requirements_viewer.txt"
    Start-Process -NoNewWindow -Wait -WorkingDirectory "piptmp" -FilePath "bin/python" -ArgumentList "pip.pyz install --target=../$sitepath --only-binary :all: --platform win32 --python-version $pyver -r requirements_viewer.txt"
}

Remove-Item -Recurse "piptmp"

# extract important parts of Lifeblood
Move-Item "$lbsrcdir/src/lifeblood" -Destination .

Write-Host "cleaning up..."
Remove-Item -Recurse "$lbsrcdir"

Pop-Location

# create launch shortcuts
Write-Host "creating links..."
if(Test-Path current){
    Remove-Item current
}
$current = 'current'
New-Item -ItemType SymbolicLink -Path $current -Target $hash
# may not have permissions
if(-not $?){
    $current = $hash
}

'@echo off','%~dp0\current\bin\python -m lifeblood.launch %*' | Set-Content -Path  "lifeblood.cmd"
if($install_viewer){
    '@echo off','%~dp0\current\bin\python -m lifeblood_viewer.launch %*' | Set-Content -Path  "lifeblood_viewer.cmd"
}

Write-Host "DONE"
Write-Host ""
Write-Host "you can now use lifeblood with provided 'lifeblood' and 'lifeblood_viewer' files."
Write-Host "just type 'lifeblood --help' and see the help message, or see documentation at gitlab"
Write-Host "you can link these files to your .local/bin, or to your /usr/local/bin to have them available in PATH"
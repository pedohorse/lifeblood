# =========================
# = Configuration Section =
# =========================
$branch = "dev"
$pyver = "3.10.9"
# ================================
# = End of Configuration Section =
# ================================

$pyver -match "\d+.\d+"
$pyxy = $Matches.0
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
})

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
Write-Host $hash

New-Item $hash -ItemType "directory"

Move-Item temp_python_bin -Destination $hash/bin
Expand-Archive "$branch.zip" -DestinationPath $hash
$lbsrcdir = "lifeblood-$branch"

Push-Location $hash

'include-system-site-packages = false' | Out-File pyvenv.cfg

$sitepath = "lib/python$pyxy/site-packages"
New-Item $sitepath -ItemType "directory"

New-Item "piptmp" -ItemType "directory"
(New-Object System.Net.WebClient).DownloadFile("https://bootstrap.pypa.io/pip/pip.pyz", "$hash/piptmp/pip.pyz")  # why that path after pushd? no idea!
Start-Process -NoNewWindow -Wait -WorkingDirectory "piptmp" -FilePath "bin/python" -ArgumentList "pip.pyz download --only-binary :all: --platform win32 --python-version $pyver lifeblood"

$whlfiles = Get-ChildItem "piptmp" -Filter "*.whl"

foreach($file in $whlfiles){
    Write-Host $file
    Expand-Archive $file -DestinationPath $sitepath
}

Remove-Item -Recurse "piptmp"

# extract important parts of Lifeblood
Move-Item "$lbsrcdir/src/lifeblood" -Destination .

Pop-Location

# create launch shortcuts
if(Test-Path current){
    Remove-Item current
}
New-Item -ItemType SymbolicLink -Path current -Target $hash
'current/bin/python -m lifeblood %*' | Out-File "lifeblood.cmd"
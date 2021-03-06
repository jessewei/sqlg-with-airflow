# Inst-addin.ps1
# Written by Jesse Wei
# LastUpdate: 2014-12-30
# Version 1.0

[CmdletBinding()]
param(
    [String] $ADDIN_NAME="vtFileMerge-V1.2",
#    [String] $SRC_DIR="",
    [String] $DEST_DIR=""
    )
function get-tempname($path) {
    do {
      $tempname = join-path $path ([IO.Path]::GetRandomFilename())
    }
    while (test-path $tempname)
    $tempname
}
   
function udfAddin([string]$name, [string]$Src, [string]$Dest)
{
    #write-host ($name, $Src, $Dest)
    $Excel = new-object -comobject excel.application
    $filename  = $name + ".xla"
    $ExcelWorkbook = $Excel.Workbooks.Add()
    if (($ExcelWorkbook.Application.AddIns | Where-Object {$_.name -eq  $filename}) -eq $null) {
        cp -Force $Src $Dest
        $AddinFile = $Dest + $filename
        #Write-Host ("Filename:", $AddinFile)
        $ExcelAddin = $ExcelWorkbook.Application.AddIns.Add("$AddinFile", $True)
        $ExcelAddin.Installed = "True"
        Write-Host "$name added"
        }
    else    {
        #Write-Host ("BF:  Addin[" + $name + "]:" + $ExcelWorkbook.Application.AddIns.Item($name).Installed)
        $ExcelWorkbook.Application.AddIns.Item($name).Installed  = "False"
        cp -Force $Src $Dest
        $ExcelWorkbook.Application.AddIns.Item($name).Installed  = "True"
        #Write-Host ("AFT: Addin[" + $name + "]:" + $ExcelWorkbook.Application.AddIns.Item($name).Installed)
        Write-Host "$name updated"        
    }    
    $Excel.Quit()
}    
function udfQAT([string]$name, [string]$Src, [string]$Dest) {

    $SrcFile = $Src+$name
    $DestFile = $Dest+$name
    $file = get-childitem $SrcFile
    
    $result = test-path -path "$Dest*" -include $name
    if ($result -like "False"){
        Write-Host "Qucick Access Toolbar(QAT) Added"        

    #   Build an array of Regex options and create the Regex object.      
        #write-host "DEBUG:", $ADDIN_DIR , $ADDIN_DEST
        
        $text = Get-Content $SrcFile
        $text -replace $ADDIN_DIR_S , $ADDIN_DEST | Out-File $DestFile    
        #$text -replace $ADDIN_DIR_S , $ADDIN_DEST     
    
    }    
    else {
        Write "Qucick Access Toolbar(QAT) skip update,", "$Dest$name Exist "    
    }
}
#
# Main program start here
# 
write-host "Installing SQL Gen Addin...."

$ADDIN_NAME = "vtFileMerge-V1.2" 
$ADDIN_DIR_S = "{ADDIN_DIR}"
$QATFILE = "Excel.officeUI"

#$SRC_DIR = "C:\Proj\SQLG\Shared\"
#$DEST_DIR = "C:\Users\IBM_ADMIN\Desktop\excel-addin\"


if ($SRC_DIR -eq "") {
    $SRC_DIR = ".\"
}

if ($DEST_DIR -eq "") {
    $ADDIN_DEST = $Env:appdata + "\Microsoft\AddIns\" 
    $ADDIN_DEST = '"{0}"' -f $ADDIN_DEST
}
else {
    $ADDIN_DEST = $DEST_DIR
}

$SRC = $SRC_DIR +  "*.xla"
$SRC_CFG = $SRC_DIR +  "*.cfg"
$QATDEST = $Env:localappdata + "\Microsoft\Office\"

# Copy configuration file
cp -Force $SRC_CFG $ADDIN_DEST

udfAddin $ADDIN_NAME $SRC $ADDIN_DEST
udfQAT "Excel.officeUI" $SRC_DIR $QATDEST

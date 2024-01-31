param (
    [string]$solutionpath="",
    [string]$solutionname = "mysolution"

)


Write-Output "Solution Name " $solutionname
Write-Output "Solution Path " $projectpath

Set-Location ../odl/code_generation_framework/type3_dwh_to_usecase/code_generator
py ./oogway.py -p $projectpath -a init -n $solutionpath

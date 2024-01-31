$path = $PWD.path

#$argumentList="..\database"
#Invoke-Expression  "& `"$scriptpath`" $argumentList"

Set-Location ../
$projectpath = $PWD.path



Set-Location ../odl/code_generation_framework/type3_dwh_to_usecase/code_generator
py ./oogway.py -p $projectpath -a build
Set-Location $path

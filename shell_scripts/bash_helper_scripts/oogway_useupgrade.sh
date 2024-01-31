# important. Do not move this file
# important. odl repo must have the same parent directory as this repo.
echo 'Make sure you have cdk dependencies installed and are using a virtual environment'
echo 'Could for instance be: source ~/repo/.venv/bin/activate'
start_path=$(pwd)
cd ..
repo_path=$(pwd)
cd ../odl/code_generation_framework/type3_dwh_to_usecase/code_generator/
python3 oogway.py -p $repo_path -a useupgrade
cd $start_path
echo 'Possible warning, replacement tags not replaced but copied to destination folder in the following files:'
grep '[:R' ../* -F -r --exclude-dir={'cdk.out','bash_helper_scripts'}


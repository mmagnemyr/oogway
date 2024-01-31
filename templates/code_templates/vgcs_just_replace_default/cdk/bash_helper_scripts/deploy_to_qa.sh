start_path=$(pwd)
cd ..
sed -i 's/761560768765/658537815488/' cdk.json
sed -i 's/"prod"/"qa"/' cdk.json
repo_path=$(pwd)
cdk deploy --all --profile qa 
cd $start_path

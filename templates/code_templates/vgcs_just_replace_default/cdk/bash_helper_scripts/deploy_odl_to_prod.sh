start_path=$(pwd)
cd ..
sed -i 's/658537815488/761560768765/' cdk.json
sed -i 's/"qa"/"prod"/' cdk.json
repo_path=$(pwd)
cdk deploy --all --profile prod
cd $start_path


Oogway
-------------------------------------------------------
Initialzie a new solution

use py ./oogwaypy -p [path to your solution folder] -a init
```mermaid
graph TD;

    oogway_repository_files --> Solution_folder

    oogway_repository_files --> solution.xlsx 

    solution.xlsx --> solution_meta_data_folder
    
   
```
Design Time
This is where you design your tables.
use py ./oogwaypy -p [path to your solution folder] -a add [table_name] to add a new table to your solution.
```mermaid
graph TD;

    User_Input --> solution.xlsx
    User_Input --> add_new_tables.xlsx
    solution.xlsx --> solution_meta_data_folder
    add_new_tables.xlsx --> solution_meta_data_folder

```
Build
py ./oogway.py p [path_to_your_soltion_folder] -a build

```mermaid
graph TD;
    
    Oogway_repository --> code_templates
    Oogway_repository --> doc_templates
    solution_meta_data_folder --> Oogway_build
    code_templates --> Oogway_build
    doc_templates --> Oogway_build
    Oogway_build --> solution_code
    Oogway_build --> solution_documentation
    solution_code --> solution_deployment
    
```
Deploy
```mermaid
graph TD;
    solution_deployment --> git
    git --> pipelines
    pipelines --> target_platform
    
```
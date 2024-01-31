py -m pydoc -w oogway_classes
Copy-Item -Path .\oogway_classes.html -Destination .\pydoc\ -Force
Remove-Item .\oogway_classes.html

py -m pydoc -w oogway_entities
Copy-Item -Path .\oogway_entities.html -Destination .\pydoc\ -Force
Remove-Item .\oogway_entities.html

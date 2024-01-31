Oogway - The big picture  
----------------------------------------------------------------------------------------
What is oogway?
   

 - A tool that will take meta data to implementation
 - Using meta data provided by you it will generate:
     - sql (Create and populate tabels using methods like Dimensional Modelling, Data Valut, 3NF, or anythng else?)
      - orchestration code (for the tool you intend to use in your target platform. AWS Steps, Data Factory, AirFlow?)
      - html documentation (or any other format you prefer... .md?)

FAQ
    Q: Do I need oogway? I use data build tool (dbt)
    A: oogway will help you build sql statements from your meta data. so you can use oogway to create input scripts to DBT

    Q: How do I enter the meta data that oogway needs?
    A: There are excel templates. So you use excel.

    Q: Does it support Kimball Dimensinal Modelling with dim and facts?
    A: Yes

    Q: Data Vault supported as well?
    A: Yes

    Q: What Cloud Providers are supported?
    A: oogway has currently support for AWS, Azure and GCP, with or without Databricks.
    
    Q: Can I extend oogway so that it supports new kind of outputs?
    A: Yes, that is how oogway is built. It is using your *templates*! 

---------------------------------------------------------------------------------------------------------------------
Thank you for using Oogway. Happy coding!```mermaid

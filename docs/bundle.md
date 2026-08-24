# Databricks Asset Bundle shape

`databricks.yml` includes two production job templates.

- `zingg_native_dedicated_photon`: real patched Zingg main class on a Photon job cluster, with native-core and patched-Zingg JARs.
- `zingg_native_serverless`: `DatabricksZinggMain` bootstrap on Serverless environment 5, with launcher, native-core, and patched-Zingg JARs.

Supply `zingg_main_class` with a bundle variable. Add the normal arguments required by that real Zingg command to the task when adapting the template to an application. The adapter deliberately does not invent or replace Zingg CLI arguments.

## Byzer-Simple-Auth

Byzer-simple-auth is a simple authentication extension which storage is based on FileSystem
for [Byzer](http://github.com/byzer-org/byzer-lang).

## Installation

1. Download the latest release
   from [here](http://store.mlsql.tech/run?action=downloadPlugin&pluginType=MLSQL_PLUGIN&pluginName=byzer-simple-auth-3.3&version=0.1.0-SNAPSHOT).
2. Move the downloaded jar file to the plugin directory of Byzer.
3. Modify `conf/byzer.properties.overwrite` file of Byzer, add the following configurations:

   ```properties
   # Configure entry class for authentication 
   streaming.plugin.clzznames=tech.mlsql.plugins.auth.simple.app.ByzerSimpleAuthApp
   # Configure authentication implementation class 
   spark.mlsql.auth.implClass=tech.mlsql.plugins.auth.simple.app.ByzerSimpleAuth 
   # Configure the directory of the auth configuration file
   spark.mlsql.auth.simple.dir=./examples/auth/
   ```

4. Restart Byzer (./bin/byzer.sh restart) with the new configuration , the extension will be loaded automatically and
   take effect.
5. Optional: There is a configuration in Byzer Notebook(conf/notebook.properties) that can be used to control the auth
   is enabled.

   ```properties
    # Enable authentication
    notebook.mlsql.skipAuth=false
   ```

## Usage

### Command way in Notebook

Add a resource which declare who can access or not allowed to access：

```shell
!simpleAuth resource add _ -type file -path "s3a://bucket7/tmp/jack" -denies allwefantasy;
```

The above command means that all users except `allwefantasy` are allowed to access the
resource `s3a://bucket7/tmp/jack`.

```shell
!simpleAuth resource add _ -type file -path "s3a://bucket7/tmp/jack" -allows allwefantasy;
```

The above command means that only `allwefantasy` is allowed to access the resource `s3a://bucket7/tmp/jack`.

You can also use yaml format to add a resource:

```shell
!simpleAuth resource add '''

apiVersion: auth.byzer.org/v1
kind: Auth
userView: []

resourceView:
  - metadata:
      resources:
        - name: "file"
          path: "s3a://bucket2/jack"
    rules:
      - rule:
          verbs:
            - "load"
          users:
            allows:
              - name: allwefantasy
                role: testRole
            denies:
              - name: jack
                role: testRole

''';
```

Once you add a resource, you can use the following command to reload the auth configuration:

```shell
!simpleAuth admin reload;
```

You can also use the following command to delete a resource:

```shell
!simpleAuth resource delete _ -type file -path "s3a://bucket7/tmp/jack" -allows allwefantasy;
```

Make sure that the resource parameters you want to delete is exactly the same as the resource you added.


You can also use the following command to query a resource:

```shell
!simpleAuth resource query _ -type file -path "s3a://bucket7/tmp/jack";
```

### Other type resources

You can also add other type resources, such as jdbc, hive, delta and son on.

#### JDBC

```shell
!simpleAuth resource add _ -type jdbc -path "wow.vega_datasets" -denies allwefantasy;
!simpleAuth admin reload;
```

Notice that the path of jdbc resource is composed of database.table. 
The path may be conflict in different database instances since we may connect many database instances eg. mysql, oracle, db2, etc.
We may try to fix this issue in the future.

#### Delta

```shell
!simpleAuth resource add _ -type delta -path "demo.table1" -denies allwefantasy;
!simpleAuth admin reload;
```

#### Hive

```shell
!simpleAuth resource add _ -type hive -path "default.table1" -denies allwefantasy;
!simpleAuth admin reload;
```

#### Special Resources


> Byzer-lang >= 2.3.5

This is a table which contains type,path and description of special resources.


| Type | Path | Description |
|--| ---- | ----------- |
| mlsql_system | `__auth_admin__` | This resource is used to control who can operate auth configuration. |
| mlsql_system | `__script_udf__` | This resource is used to control who can register udf  |
| mlsql_system | `__plugin_operator__` | This resource is used to who can use `!plugin` command  |
| mlsql_system | `__profiler__` | This resource is used to who can use `!profiler` command  |
     


Most plugins will use the special resources to control who can use the plugin. You can check the plugin's document to know
which special resource name it will use.


#### Auth admin

There is a special resource `mlsql_system.__auth_admin__` which is used to control who can operate auth configuration.
Normally we can provide a base configuration file which contains the resource `mlsql_system.__auth_admin__` to make sure 
only the specified users can operate auth configuration.


```yaml
apiVersion: auth.byzer.org/v1
kind: Auth
userView: [ ]
resourceView:
  - metadata:
      resources:
        - name: "mlsql_system"
          path: "__auth_admin__"
    rules:
      - rule:
          verbs:
            - "empty"
          users:
            allows:
              - name: allwefantasy
                role: testRole
```
   
Then you can use the command to add the resource access control with this user.


### Manual way of writing YAML Auth file

The auth configuration file is a YAML file, the following is an example:

```yaml
apiVersion: auth.byzer.org/v1
kind: Auth
userView: [ ]
resourceView:
  - metadata:
      resources:
        - name: "mlsql_system"
          path: "__auth_admin__"
    rules:
      - rule:
          verbs:
            - "empty"
          users:
            allows:
              - name: allwefantasy
                role: testRole
  - metadata:
      resources:
        - name: "file"
          path: "s3a://bucket2/jack"
    rules:
      - rule:
          verbs:
            - "load"
          users:
            allows:
              - name: allwefantasy
                role: testRole
            denies:
              - name: jack
                role: testRole

  - metadata:
      resources:
        - name: "file"
          path: "s3a://bucket3/jack"
    rules:
      - rule:
          verbs:
            - "load"
          users:
            denies:
              - name: allwefantasy
                role: testRole
            allows: [ ]

```

Notice that resource `mlsql_system.__auth_admin__` is a special resource which is used to control who can operate auth
configuration,
in this example, `allwefantasy` is defined as the only user who can operate auth configuration.






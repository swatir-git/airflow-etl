def set_azure_configuration(spark, azure_storage_acc, azure_client_id, azure_client_secret, azure_tenant_id):
    spark.conf.set(f"fs.azure.account.auth.type.{azure_storage_acc}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{azure_storage_acc}.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{azure_storage_acc}.dfs.core.windows.net", azure_client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{azure_storage_acc}.dfs.core.windows.net",
                   azure_client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{azure_storage_acc}.dfs.core.windows.net",
                   f"https://login.microsoftonline.com/{azure_tenant_id}/oauth2/token")
    spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

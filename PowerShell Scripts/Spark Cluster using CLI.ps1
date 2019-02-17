# this is the command used to create a Spark cluster that uses ADLS Gen2 along with managed identity

az hdinsight create -g DataLake -n sparkcluster2019 `
                        --type Spark `
                        --location westus2 `
                        --assign-identity HadoopIdentity `
                        --http-user admin `
                        --http-password BLAHBLAH `
                        --cluster-tier Standard `
                        --storage-account name `
                        --storage-account-key key `
                        --storage-default-filesystem yousryfs `
                        --subscription subscription`
                        --version 3.6 `
                        --workernode-size Standard_D13_V2 `
                        --size 4

	const connectionString = "<azure-cosmos-db-nosql-connection-string>"

	clientOptions := azcosmos.ClientOptions{
		EnableContentResponseOnWrite: true,
	}
	
	client, err := azcosmos.NewClientFromConnectionString(connectionString, &clientOptions)
	if err != nil {
		return err
	}

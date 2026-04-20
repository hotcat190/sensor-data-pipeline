# Installation
- `docker-compose up -d`
- run `init.sql` (see [useful_command](useful_command.txt))
- connect clickstack and grafana:
    - grafana username and pass: admin
    - install clickhouse plugin
    - add data source
        - server: clickstack
        - port: 8123
        - protocol: http
        - user: grafana
        - pass: 123
		
# Setup Nifi:
- Login information:
	- URL: https://localhost:8443/nifi
	- Username: admin
	- Password: Password1234
- Setup flow registry:
	- In the nifi GUI, click on the ☰ symbol on the top-right and click Controller Settings
	- Switch to the Registry Clients and click + (Add)
	- Choose type as GitHubFlowRegistryClient and click Add
	- Click on the 3 dots on the right > Edit
	- Fill the following fields:
		- Repository Owner: hotcat190
		- Repository Name: sensor-data-pipeline
		- Authentication Type: Personal Access Token
		- Personal Access Token: <access-token>
		- Default Branch: master
		- Repository Path: nifi-flow
	- Click the Checkmark on the top-right, leave Referenced Attributes empty, then click Verify to ensure correct configuration.
	- If everything looks good, click Apply
	- Return to the Canvas by clicking the Nifi icon on the top-left or the 3 dots > Canvas on the top-right
	- Drag and Drop the Import from Registry icon from the tool bar to the canvas, the Import From Registry window will show up.
	- Pick the right Registry, Bucket and Flow, then click Import.

# Demo note
- BME:
	- compression codecs: DoubleDelta, LZ4
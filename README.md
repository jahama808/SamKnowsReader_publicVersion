# SamKnowReader

This python script performs several tasks:

1. Pull whitebox data for download tests performed over 7 days.
2. Pull whitebox data for upload tests performed over 7 days.
3. Pull whitebox data for latency tests performed over 7 days.

This data is read in as JSON and transferred into a Pandas dataframe.

During the import into the dataframe, the results are compared with the FCC thresholds,
and any violation of the threshold is counted and a percentage is calculated against all 
the performance data.  For example, if 10 download results were below 10Mbps, and there
were 100 measurement results, the percentage would be 10%.

The failure results are placed into a second dataframe that contains:

1. Customer Account #
2. Sam Knows Whitebox Serial Number
3. Sam Knows Unit ID
4. Access ID (identifying the specific access element and port the line is built on)
5. Core ID (identifying the specific core element and SAP the customer is built on)
6. Access Type (DSL vs GPON)
7. the 3 failure calculations

This dataframe is then "pushed" to a remote mySQL database.  Right now this is located
in Amazon's AWS RDS service (I needed something that I could setup quickly).  

The script runs in a separate server (outside of AWS) for now.  


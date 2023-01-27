dir="/data/dataset/"
echo "query tpcds dataset..." > filterlog.txt
for comp in uncompressed zstd lz4 snappy  zlib;
#for comp in snappy;
do
#	for table in customer_demographics customer customer_address item inventory store_sales store_returns catalog_returns catalog_sales web_sales web_returns;
	for table in catalog_sales
		do
		  if [[ $comp == "zstd" ]]
		  then
#		    for level in 1 5 9;
		    for level in 1;
		    do
		       /home/chunwei/orc-1.7.2/cmake-build-release-azure/tools/src/orc-filter -c 1  "${dir}${table}_${comp}.orc"  >> filterlog.txt
		    done
		  else
		    /home/chunwei/orc-1.7.2/cmake-build-release-azure/tools/src/orc-filter -c 1  "${dir}${table}_${comp}.orc" >> filterlog.txt
      fi
		done
done
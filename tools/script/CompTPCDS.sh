dir="/data/dataset/"
echo "start compressing tpcds dataset..." > complog.txt
for comp in uncompressed zstd lz4 zlib snappy;
#for comp in snappy;
do
	for table in customer_demographics customer customer_address item inventory store_sales store_returns catalog_returns catalog_sales web_sales web_returns;
		do
		  if [[ $comp == "zstd" ]]
		  then
#		    for level in 1 5 9;
		    for level in 1;
		    do
		       /home/chunwei/orc-1.7.2/cmake-build-release-azure/tools/src/csv-orc "${dir}${table}" ${comp} ${level} >> complog.txt
		    done
		  else
		    /home/chunwei/orc-1.7.2/cmake-build-release-azure/tools/src/csv-orc "${dir}${table}" ${comp} 1 >> complog.txt
      fi
		done
done
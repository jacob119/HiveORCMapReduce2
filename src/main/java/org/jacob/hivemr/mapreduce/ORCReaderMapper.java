package org.jacob.hivemr.mapreduce;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;

import java.io.IOException;
import java.util.List;

public class ORCReaderMapper extends Mapper<NullWritable, OrcStruct, NullWritable, Text> {

    FileSystem fs;
    TypeInfo typeInfo;
    ObjectInspector inspector;
    Text v;
    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(NullWritable key, OrcStruct value,
                       Mapper<NullWritable, OrcStruct, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {

        inspector = value.createObjectInspector(typeInfo);
        StructObjectInspector structObjectInspector = (StructObjectInspector) inspector;
        List columnValues = structObjectInspector.getStructFieldsDataAsList(value);

        String fileName = columnValues.get(0).toString();
        Text eventDate =(Text) columnValues.get(1);

        // <Your custom logic with the key and value pairs>
        v.set(fileName + "  "+ eventDate.toString());
        context.write(NullWritable.get(), v);
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void setup(
            Mapper<NullWritable, OrcStruct, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub

        typeInfo = TypeInfoUtils.getTypeInfoFromTypeString("struct<resource:string,evttime:date>");

        v = new Text();

        super.setup(context);
    }

}

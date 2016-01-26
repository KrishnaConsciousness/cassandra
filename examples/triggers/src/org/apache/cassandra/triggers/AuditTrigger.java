package org.apache.cassandra.triggers;

/**
 * Created by dash on 11/2/16.
 */


        import java.io.InputStream;
        import java.io.PrintWriter;
        import java.io.StringWriter;
        import java.io.UnsupportedEncodingException;
        import java.nio.ByteBuffer;
        import java.nio.charset.CharacterCodingException;
        import java.util.ArrayList;
        import java.util.Collection;
        import java.util.List;
        import java.util.Properties;

        import org.apache.cassandra.config.CFMetaData;
        import org.apache.cassandra.config.Schema;
        import org.apache.cassandra.db.composites.CellName;
        import org.apache.cassandra.db.composites.CellNameType;
        import org.apache.cassandra.db.marshal.UUIDType;
        import org.apache.cassandra.utils.ByteBufferUtil;
        import org.apache.cassandra.utils.UUIDGen;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import org.apache.cassandra.db.Cell;
        import org.apache.cassandra.db.ColumnFamily;
        import org.apache.cassandra.db.Mutation;
        import org.apache.cassandra.io.util.FileUtils;

public class AuditTrigger implements ITrigger {
    private static final Logger logger = LoggerFactory.getLogger(AuditTrigger.class);
    private Properties properties = loadProperties();

    public Collection<Mutation> augment(ByteBuffer key, ColumnFamily update)
    {
        CFMetaData cfm = update.metadata();

        List<Mutation> mutations = new ArrayList<>(update.getColumnCount());

        String keyspaceName = "";
        String tableName    = "";
        String keyStr       = "";

        keyspaceName = cfm.ksName;
        tableName = cfm.cfName;

        try {
            keyStr = ByteBufferUtil.string(key);
        } catch (CharacterCodingException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }

        for (Cell cell : update)
        {
            // Skip the row marker and other empty values, since they lead to an empty key.
            if (cell.value().remaining() > 0)
            {
                CFMetaData other = Schema.instance.getCFMetaData("test","audit");
                CellNameType cnt = other.comparator;

                ByteBuffer auditkey = UUIDType.instance.decompose(UUIDGen.getTimeUUID());

                // create CellName objects for each of the columns in the audit table row we are inserting
                CellName primaryKeyCellName = cnt.makeCellName("primary_key");
                CellName keyspaceCellName = cnt.makeCellName("keyspace_name");
                CellName tableCellName = cnt.makeCellName("table_name");

                try {
                    // put the values we want to write to the audit table into ByteBuffer objects
                    ByteBuffer ksvalbb,tablevalbb,keyvalbb;
                    ksvalbb=ByteBuffer.wrap(keyspaceName.getBytes("UTF8"));
                    tablevalbb=ByteBuffer.wrap(tableName.getBytes("UTF8"));
                    keyvalbb=ByteBuffer.wrap(keyStr.getBytes("UTF8"));

                    // create the mutation object
                    Mutation mutation = new Mutation(keyspaceName, auditkey);

                    // get the time which will be needed for the call to mutation.add
                    long mutationTime=System.currentTimeMillis();

                    // add each of the column values to the mutation
                    mutation.add("audit", primaryKeyCellName, keyvalbb,  mutationTime);
                    mutation.add("audit", keyspaceCellName,  ksvalbb,  mutationTime);
                    mutation.add("audit", tableCellName, tablevalbb,  mutationTime);

                    mutations.add(mutation);
                } catch (UnsupportedEncodingException e) {
                    StringWriter errors = new StringWriter();
                    e.printStackTrace(new PrintWriter(errors));
                    logger.error(errors.toString());
                }
            }
        }
        return mutations;
    }

    private static Properties loadProperties()
    {
        Properties properties = new Properties();
        InputStream stream = AuditTrigger.class.getClassLoader().getResourceAsStream("AuditTrigger.properties");
        try
        {
            properties.load(stream);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }
        logger.info("loaded property file, AuditTrigger.properties");
        return properties;
    }

}

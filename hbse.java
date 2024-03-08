import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.json.JSONObject;

public class HBaseJsonConverter {

    public static JSONObject convertResultToJson(Result result) {
        JSONObject json = new JSONObject();

        for (Cell cell : result.rawCells()) {
            String columnFamily = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            String qualifier = new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            String key = columnFamily + ":" + qualifier;
            json.put(key, value);
        }

        return json;
    }

    public static void main(String[] args) {
        // Assuming you have a Result object named "result"
        // Result result = ...;

        // Convert the Result to JSON
        JSONObject json = convertResultToJson(result);

        // Print the JSON representation
        System.out.println(json.toString());
    }
}

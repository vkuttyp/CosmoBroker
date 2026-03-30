using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

var connStr = args.Length > 0 ? args[0]
    : "Server=localhost,1433;Database=ZatcaDb;User Id=sa;Password=aBCD111;TrustServerCertificate=True;";

var subject = args.Length > 1 ? args[1] : null;   // e.g. "1-E-2/26"
var limit   = args.Length > 2 ? int.Parse(args[2]) : 5;

await using var conn = new SqlConnection(connStr);
await conn.OpenAsync();

var sql = subject != null
    ? $"""
        SELECT TOP {limit} id, stream_name, subject, payload
        FROM mq_messages
        WHERE stream_name = 'ZATCA_RESPONSES'
          AND subject = @subject
        ORDER BY id DESC
        """
    : $"""
        SELECT TOP {limit} id, stream_name, subject, payload
        FROM mq_messages
        WHERE stream_name = 'ZATCA_RESPONSES'
        ORDER BY id DESC
        """;

await using var cmd = new SqlCommand(sql, conn);
if (subject != null)
    cmd.Parameters.AddWithValue("@subject", subject);

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    long id         = reader.GetInt64(0);
    string streamNm = reader.IsDBNull(1) ? "(null)" : reader.GetString(1);
    string subj     = reader.GetString(2);
    byte[] raw      = (byte[])reader.GetValue(3);
    string json;
    try
    {
        string b64 = Encoding.ASCII.GetString(raw).Trim();
        json = Encoding.UTF8.GetString(Convert.FromBase64String(b64));
    }
    catch
    {
        json = Encoding.UTF8.GetString(raw);
    }

    // Pretty-print so long fields (SignedXml, ApiResponse) don't get truncated
    // by terminal scroll-buffer line-length limits.
    try
    {
        using var doc = JsonDocument.Parse(json);
        json = JsonSerializer.Serialize(doc, new JsonSerializerOptions { WriteIndented = true });
    }
    catch { /* leave as-is if not valid JSON */ }

    Console.WriteLine($"=== id={id}  stream={streamNm}  subject={subj}  bytes={raw.Length} ===");
    Console.WriteLine(json);
    Console.WriteLine();
}

#define BRIDGE_PLUGIN
#include "persistence_plugin.h"
#include "sqlite3.h"
#include <functional>
// We can pretend this is sufficient for migration logic(it probably actually is).
// This is distinct from the schema_version pragma in sqlite, as that is automatically
// incremented with changes made to the schema
const int SCHEMA_VERSION = 1;
thread_local sqlite3* dbconn = nullptr;
namespace
{
    void create_tables(sqlite3* connection)
    {
        char* errormsg = nullptr;
        int current_version;
        auto capture_version = [](void* current_version, int column_count, char** column_values, char** column_names) {
            sscanf(column_values[0], "%i", (int*) current_version);
            return 0;
        };
        sqlite3_exec(connection, "PRAGMA user_version", capture_version, &current_version, &errormsg);

        if (current_version <= 1) {
            sqlite3_exec(
                connection,
                "CREATE TABLE IF NOT EXISTS key_values(key BLOB PRIMARY KEY, value BLOB)",
                nullptr,
                nullptr,
                &errormsg);
            sqlite3_exec(
                connection,
                "CREATE TABLE IF NOT EXISTS multipart_uploads(id INTEGER PRIMARY KEY,"
                "                                             path TEXT UNIQUE,"
                "                                             user TEXT)",
                nullptr,
                nullptr,
                &errormsg);
            sqlite3_exec(
                connection,
                "CREATE TABLE IF NOT EXISTS multipart_upload_parts(multipart_id INTEGER,"
                "                                                  part_number  INTEGER,"
                "                                                  PRIMARY KEY(multipart_id,part_number),"
                "                                                  FOREIGN KEY(multipart_id)"
                "                                                          REFERENCES multipart_uploads.id)",
                nullptr,
                nullptr,
                &errormsg);
            sqlite3_exec(connection, "PRAGMA user_version=1", nullptr, nullptr, &errormsg);
        }
    }
    sqlite3* initialize_db()
    {
        static thread_local sqlite3* connection = nullptr;
        if (not connection) {
            sqlite3_open("s3-bridge.db", &connection);
        }
        return connection;
    }
} // namespace
extern "C" void plugin_initialize(rcComm_t* connection, const char* configuration)
{
}

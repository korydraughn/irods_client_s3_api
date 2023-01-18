#define BRIDGE_PLUGIN
#include "persistence_plugin.h"
#include "sqlite3.h"
#include <mutex>
#include <nlohmann/json.hpp>

/// We can pretend this is sufficient for migration logic(it probably actually is).
/// This is distinct from the `schema_version` pragma in sqlite, as that is automatically
/// incremented with changes made to the schema
const int SCHEMA_VERSION = 1;

namespace
{

    std::mutex database_creation_mutex;

    /// Create the tables for the database, migrating it forwards if necessary
    void create_tables(sqlite3* connection)
    {
        // We very much don't need multiple migrations running at once.
        // Grabbing the mutex initially prevents other threads from
        // intermingling with this one.
        std::lock_guard<std::mutex> guard(database_creation_mutex);

        char* errormsg = nullptr;
        int current_version;
        auto capture_version = [](void* current_version, int column_count, char** column_values, char** column_names) {
            *(int*) current_version = strtol(column_values[0], nullptr, 10);
            // This may need error checking to ensure that it worked properly.
            // That shouldn't happen in this case, but you know how it goes.
            return SQLITE_OK;
        };
        sqlite3_exec(connection, "PRAGMA user_version", capture_version, &current_version, &errormsg);
        if (current_version == SCHEMA_VERSION) {
            return;
        }
        int ec;
        if (current_version < 1) {
            ec = sqlite3_exec(
                connection,
                "CREATE TABLE IF NOT EXISTS key_values(key BLOB PRIMARY KEY, value BLOB)",
                nullptr,
                nullptr,
                &errormsg);
            if (ec != 0) {
            }
            ec = sqlite3_exec(
                connection,
                "CREATE TABLE IF NOT EXISTS multipart_uploads(id INTEGER PRIMARY KEY,"
                "                                             path TEXT UNIQUE,"
                "                                             user TEXT)",
                nullptr,
                nullptr,
                &errormsg);
            if (ec != 0) {
            }
            ec = sqlite3_exec(
                connection,
                "CREATE TABLE IF NOT EXISTS multipart_upload_parts(multipart_id INTEGER,"
                "                                                  part_number  INTEGER,"
                "                                                  PRIMARY KEY(multipart_id,part_number),"
                "                                                  FOREIGN KEY(multipart_id)"
                "                                                          REFERENCES multipart_uploads.id)",
                nullptr,
                nullptr,
                &errormsg);
            if (ec != 0) {
            }
            // Update the schema to avoid rerunning this version.
            ec = sqlite3_exec(connection, "PRAGMA user_version=1", nullptr, nullptr, &errormsg);
            if (ec != 0) {
            }
        }
    }

    /// Create a new multipart upload and obtain an unused ID for it.
    /// \returns The new upload id, or a nullptr when the creation fails.
    char* create_multipart_upload(rcComm_t* connection, sqlite3* db, const std::string_view& path)
    {
        sqlite3_stmt* insertion;
        int ec;

        // For safety purposes we need to prepare a statement to insert here.

        ec = sqlite3_prepare_v2(db, "INSERT INTO multipart_uploads(path,user) values (?,?)", -1, &insertion, nullptr);
        if (ec != SQLITE_OK) {
            // Make a big noise when it falls over
            throw "AAAAA";
            sqlite3_finalize(insertion);
            // TODO error handling here.
            return nullptr;
        }
        // The binding requires the number of bytes in it, rather than the number of characters in the string.
        // I understand that the null character is meant to be included in this, but sqlite3 isn't very picky about
        // differences between BLOB and TEXT objects.
        // Well, not by default at least 😉
        ec = sqlite3_bind_text(
            insertion, 0, connection->clientUser.userName, strlen(connection->clientUser.userName) + 1, nullptr);
        if (ec != SQLITE_OK) {
        }
        ec = sqlite3_bind_text(insertion, 1, path.data(), path.length() + 1, nullptr);
        if (ec != SQLITE_OK) {
        }

        // This is where the insertion is actually performed.
        ec = sqlite3_step(insertion);
        if (ec != SQLITE_OK) {
            // Do something you something or other
        }
        sqlite3_finalize(insertion);
        char* result_id = nullptr;
        sqlite3_exec(
            db,
            "select last_insert_rowid()",
            [](void* result_id, int columns, char** column_values, char** column_names) {
                *static_cast<char**>(result_id) = strdup(column_values[0]);
                return SQLITE_OK;
            },
            &result_id,
            nullptr);
        return result_id;
    }
    bool create_upload_part(
        rcComm_t* connection,
        sqlite3* db,
        const std::string_view& upload_id,
        const std::string_view& part_id)
    {
        sqlite3_stmt* insertion;
        int ec = sqlite3_prepare_v2(
            db,
            "INSERT INTO MULTIPART_UPLOAD_PARTS(multipart_id, part_number)"
            "            VALUES(?,?)",
            -1,
            &insertion,
            nullptr);
        if (ec != SQLITE_OK) {
            return false;
        }
        // The sqlite type system performs conversions to the column type if possible.
        // if this is a problem in practice, then we can look at changing the table declarations to be
        // strict, which will result in anything that cannot be converted to the column type
        // triggering an SQLITE_CONSTRAINT_DATATYPE error.
        ec = sqlite3_bind_text(insertion, 0, upload_id.data(), upload_id.length() + 1, nullptr);
        ec = sqlite3_bind_text(insertion, 1, part_id.data(), part_id.length() + 1, nullptr);
        sqlite3_step(insertion);
        sqlite3_finalize(insertion);
        return true;
    }

    sqlite3* initialize_db()
    {
        // The idea is that each thread of the runtime only needs one connection
        // and that it only needs to be initialized once, so we can avoid all these
        // goofy calls when we can avoid them
        static thread_local sqlite3* connection = nullptr;
        if (not connection) {
            int ec = sqlite3_open_v2(
                "s3-bridge.db",
                &connection,
                // Permit opening it or creating the database. Do not permit opening symbolic links.
                // Open the database with extended error codes enabled.
                SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOFOLLOW | SQLITE_OPEN_EXRESCODE,
                // Use the default virtual filesystem, we're not doing anything that needs that, I hope
                nullptr);
            if (ec != SQLITE_OK) {
                throw sqlite3_errmsg(connection);
            }
            // This setting substantially improves disk IO characteristics and reduces
            // the number of fsync calls necessary.
            // It also helps prevent readers from blocking writers and visa-versa.
            // See https://www.sqlite.org/wal.html for details
            sqlite3_exec(connection, "PRAGMA journal_mode = WAL", nullptr, nullptr, nullptr);

            // Set a reasonable timeout for queries to be reattempted.
            // 100ms plus or minus some change seems reasonable enough
            sqlite3_busy_timeout(connection, 100 + random() % 20);

            // Finally with all that done, make the tables.
            create_tables(connection);
        }

        return connection;
    }

} // namespace
extern "C" void plugin_initialize(rcComm_t* connection, const char* config)
{
    nlohmann::json configuration = config;
}

#include "plugin.hpp"
#include "persistence_plugin.h"
#include "persistence.hpp"

namespace
{
    struct persistence_plugin
    {
        multipart_create_fn create_fn;
        multipart_complete_fn complete_fn;
        multipart_abort_fn abort_fn;
        multipart_list_parts_fn list_parts_fn;
        multipart_list_uploads_fn list_uploads_fn;
        store_key_value_fn store_key_fn;
        get_key_value_fn get_key_value_fn;
    } active_persistence_plugin;
} //namespace

void add_persistence_plugin(
    multipart_create_fn create_fn,
    multipart_complete_fn complete_fn,
    multipart_abort_fn abort_fn,
    multipart_list_parts_fn list_parts_fn,
    multipart_list_uploads_fn list_uploads_fn,
    store_key_value_fn store_key_fn,
    get_key_value_fn get_value_fn)
{
    active_persistence_plugin = persistence_plugin{
        create_fn, complete_fn, abort_fn, list_parts_fn, list_uploads_fn, store_key_fn, get_value_fn};
}

void free_multipart_result(multipart_listing_output* c)
{
    std::free(c->key);
    std::free(c->owner);
    std::free(c->upload_id);
}
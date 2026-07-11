/* C ABI smoke test: exercises the liberime call surface through rime_api.h
 * idioms (RIME_STRUCT / data_size / free_*).  Link it against either the shim
 * (dist/librime.1.dylib) or the real librime and compare outputs field by field.
 *
 *   cc -I$(brew --prefix)/include tests/smoke.c dist/librime.1.dylib -o smoke-shim
 *   cc -I$(brew --prefix)/include tests/smoke.c -L$(brew --prefix)/lib -lrime -o smoke-real
 *   ./smoke-shim <shared_data_dir> <user_data_dir>
 */
#include <stdio.h>
#include <rime_api.h>

int main(int argc, char **argv) {
  if (argc < 3) {
    fprintf(stderr, "usage: %s <shared_data_dir> <user_data_dir> [key_sequence]\n", argv[0]);
    return 2;
  }
  const char *shared = argv[1];
  const char *user = argv[2];
  const char *keys = argc > 3 ? argv[3] : "nihao";

  RimeApi *api = rime_get_api();
  printf("data_size=%d version=%s\n", api->data_size, api->get_version());

  RIME_STRUCT(RimeTraits, t);
  t.shared_data_dir = shared;
  t.user_data_dir = user;
  t.app_name = "rime.smoke";
  api->setup(&t);
  api->initialize(&t);
  api->set_notification_handler(NULL, NULL);
  if (api->start_maintenance(False))
    api->join_maintenance_thread();

  RimeSessionId sid = api->create_session();
  printf("session-ok=%d find=%d\n", sid != 0, api->find_session(sid));

  api->simulate_key_sequence(sid, keys);
  RIME_STRUCT(RimeContext, ctx);
  if (api->get_context(sid, &ctx)) {
    printf("preedit=[%s] num=%d page_size=%d\n", ctx.composition.preedit,
           ctx.menu.num_candidates, ctx.menu.page_size);
    for (int i = 0; i < ctx.menu.num_candidates; i++)
      printf("  %d. %s%s%s\n", i + 1, ctx.menu.candidates[i].text,
             ctx.menu.candidates[i].comment ? " ; " : "",
             ctx.menu.candidates[i].comment ? ctx.menu.candidates[i].comment : "");
    api->free_context(&ctx);
  } else {
    printf("get_context FAILED\n");
    return 1;
  }

  RimeCandidateListIterator it = {0};
  if (api->candidate_list_from_index(sid, &it, 0)) {
    int n = 0;
    while (api->candidate_list_next(&it) && n < 8) {
      printf("iter[%d]=%s\n", it.index, it.candidate.text);
      n++;
    }
    api->candidate_list_end(&it);
  }

  RIME_STRUCT(RimeStatus, st);
  if (api->get_status(sid, &st)) {
    printf("schema=%s/%s composing=%d ascii=%d\n", st.schema_id, st.schema_name,
           st.is_composing, st.is_ascii_mode);
    api->free_status(&st);
  }

  char cur[64];
  if (api->get_current_schema(sid, cur, sizeof(cur)))
    printf("current_schema=%s\n", cur);

  api->select_candidate_on_current_page(sid, 0);
  RIME_STRUCT(RimeCommit, commit);
  if (api->get_commit(sid, &commit)) {
    printf("commit=%s\n", commit.text);
    api->free_commit(&commit);
  }

  /* schema config through the handle proxy */
  RimeConfig cfg = {0};
  if (api->schema_open(cur, &cfg)) {
    const char *name = api->config_get_cstring(&cfg, "schema/name");
    int page = 0;
    api->config_get_int(&cfg, "menu/page_size", &page);
    printf("schema_config name=%s page_size=%d\n", name ? name : "(null)", page);
    api->config_close(&cfg);
  } else {
    printf("schema_open FAILED\n");
  }

  api->destroy_session(sid);
  api->finalize();
  printf("smoke ok\n");
  return 0;
}

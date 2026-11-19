#include "workload_tags.h"

#include <usual/string.h>

static void expect(const char *sql, size_t length, bool complete,
		   enum WorkloadTagStatus expected, const char *team, const char *caller)
{
	struct WorkloadTags tags;
	enum WorkloadTagStatus actual;

	memset(&tags, 'X', sizeof(tags));
	actual = workload_parse_tags(sql, length, complete, &tags);
	if (actual != expected || strcmp(tags.team, team) || strcmp(tags.caller, caller)) {
		fprintf(stderr, "tag test failed: status %d, expected %d\n", actual, expected);
		exit(1);
	}
}

int main(void)
{
	static const struct {
		const char *sql;
		enum WorkloadTagStatus status;
		const char *team;
		const char *caller;
	} cases[] = {
		{"SELECT 1", WORKLOAD_TAGS_MISSING, "", ""},
		{"SELECT '/*team=x,caller=y*/'", WORKLOAD_TAGS_MISSING, "", ""},
		{"SELECT 1 /*team=x,caller=y*/", WORKLOAD_TAGS_MISSING, "", ""},
		{"/*team='one',caller='Jobs%3A%3ASync'*/SELECT 1", WORKLOAD_TAGS_VALID, "one", "Jobs::Sync"},
		{"/*team=one caller=Jobs%3a%3aSync*/SELECT 1", WORKLOAD_TAGS_VALID, "one", "Jobs::Sync"},
		{"/*team='one'*/SELECT 1", WORKLOAD_TAGS_MISSING, "one", ""},
		{"/*caller='Jobs%23run'*/SELECT 1", WORKLOAD_TAGS_MISSING, "", "Jobs#run"},
		{"/*team='',caller='unknown'*/SELECT 1", WORKLOAD_TAGS_MISSING, "", "unknown"},
		{"/*trace_id='arbitrary%ZZ'*/SELECT 1", WORKLOAD_TAGS_MISSING, "", ""},
		{"/*team=x,caller='a+b%20c'*/SELECT 1", WORKLOAD_TAGS_VALID, "x", "a+b c"},
		{"/*team=x,caller='%E2%82%AC'*/SELECT 1", WORKLOAD_TAGS_VALID, "x", "\xe2\x82\xac"},
		{"/*team=x,caller='%00'*/SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/*team=x,caller='%0A'*/SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/*team=x,caller='%ZZ'*/SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/*team=x,caller='%FF'*/SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/*team=x,caller='%C0%AF'*/SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/*team=x,caller='bad%'*/SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/*team=x,caller='bad%2'*/SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/*team=x,caller='%ED%A0%80'*/SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/*team=x,caller='a%252Fb'*/SELECT 1", WORKLOAD_TAGS_VALID, "x", "a%2Fb"},
		{"/*team=x,team=x,caller=y*/SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/*team=x*/ /*caller=y*/ SELECT 1", WORKLOAD_TAGS_VALID, "x", "y"},
		{"/*team=x*/ /*team=x,caller=y*/ SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/*team=x,caller=y oops*/SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/*team=x,caller='y*/SELECT 1", WORKLOAD_TAGS_MALFORMED, "", ""},
		{"/* a report about team=x */ SELECT 1", WORKLOAD_TAGS_MISSING, "", ""},
		{"/* prose */ /*team=x,caller=y*/ SELECT 1", WORKLOAD_TAGS_VALID, "x", "y"},
		{"/*+ index(foo) */ /*team=x,caller=y*/ SELECT 1", WORKLOAD_TAGS_VALID, "x", "y"},
		{"/* outer /*team=x,caller=y*/ */ SELECT 1", WORKLOAD_TAGS_MISSING, "", ""},
		{"-- a report\n/*team=x,caller=y*/ SELECT 1", WORKLOAD_TAGS_VALID, "x", "y"},
		{"/*TEAM=x,CALLER=y*/SELECT 1", WORKLOAD_TAGS_MISSING, "", ""},
	};
	size_t i;
	char long_query[WORKLOAD_PREFIX_MAX * 2];
	char exact[WORKLOAD_CALLER_MAX + 40];
	char *p;

	for (i = 0; i < ARRAY_NELEM(cases); i++)
		expect(cases[i].sql, strlen(cases[i].sql), true, cases[i].status, cases[i].team, cases[i].caller);
	expect("/", 1, false, WORKLOAD_TAGS_UNSUPPORTED, "", "");
	expect("/*team=x,caller=y", strlen("/*team=x,caller=y"), false, WORKLOAD_TAGS_UNSUPPORTED, "", "");
	expect("/*team=x,caller=y*/", 19, false, WORKLOAD_TAGS_UNSUPPORTED, "", "");
	expect("/*team=x,caller=y*/S", 20, false, WORKLOAD_TAGS_VALID, "x", "y");
	for (i = 0; i <= 20; i++) {
		char *fragment = malloc(i ? i : 1);

		if (!fragment)
			return 1;
		memcpy(fragment, "/*team=x,caller=y*/SELECT 1", i);
		expect(fragment, i, false,
		       i < 20 ? WORKLOAD_TAGS_UNSUPPORTED : WORKLOAD_TAGS_VALID,
		       i < 20 ? "" : "x", i < 20 ? "" : "y");
		free(fragment);
	}

	memset(long_query, ' ', sizeof(long_query));
	expect(long_query, sizeof(long_query), true, WORKLOAD_TAGS_UNSUPPORTED, "", "");
	memcpy(long_query, "/*team=x,caller=y*/SELECT ", strlen("/*team=x,caller=y*/SELECT "));
	expect(long_query, sizeof(long_query), false, WORKLOAD_TAGS_VALID, "x", "y");

	p = exact;
	strcpy(p, "/*team=x,caller='");
	p += strlen(p);
	memset(p, 'a', WORKLOAD_CALLER_MAX);
	p += WORKLOAD_CALLER_MAX;
	strcpy(p, "'*/SELECT 1");
	{
		char caller[WORKLOAD_CALLER_MAX + 1];

		memset(caller, 'a', WORKLOAD_CALLER_MAX);
		caller[WORKLOAD_CALLER_MAX] = 0;
		expect(exact, strlen(exact), true, WORKLOAD_TAGS_VALID, "x", caller);
	}
	p[0] = 'a';
	strcpy(p + 1, "'*/SELECT 1");
	expect(exact, strlen(exact), true, WORKLOAD_TAGS_UNSUPPORTED, "", "");
	return 0;
}

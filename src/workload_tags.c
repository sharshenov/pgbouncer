/*
 * PgBouncer - Lightweight connection pooler for PostgreSQL.
 *
 * Copyright (c) 2026 PgBouncer contributors
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "workload_tags.h"

#include <usual/string.h>
#include <usual/utf8.h>

struct TagParser {
	struct WorkloadTags tags;
	bool seen_team;
	bool seen_caller;
};

static bool space(unsigned char c)
{
	return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v';
}

static bool key_char(unsigned char c)
{
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
	       || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.';
}

static bool bare_value_char(unsigned char c)
{
	return key_char(c) || (c != 0 && strchr(":/@%+~", c) != NULL);
}

static int hex_value(unsigned char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	return -1;
}

static enum WorkloadTagStatus decode_value(const char *p, const char *end, char *out, size_t capacity)
{
	size_t length = 0;

	while (p < end) {
		unsigned char c = *p++;

		if (c == '%') {
			int hi, lo;

			if (end - p < 2)
				return WORKLOAD_TAGS_MALFORMED;
			hi = hex_value(p[0]);
			lo = hex_value(p[1]);
			if (hi < 0 || lo < 0)
				return WORKLOAD_TAGS_MALFORMED;
			c = hi * 16 + lo;
			p += 2;
		}
		if (c < 32 || c == 127)
			return WORKLOAD_TAGS_MALFORMED;
		if (length + 1 >= capacity)
			return WORKLOAD_TAGS_UNSUPPORTED;
		out[length++] = c;
	}
	out[length] = 0;
	return utf8_validate_string(out, out + length) ? WORKLOAD_TAGS_VALID : WORKLOAD_TAGS_MALFORMED;
}

/* Prose/hints are not tag lists; a list that starts parsing must finish cleanly. */
static enum WorkloadTagStatus parse_comment(const char *p, const char *end, struct TagParser *parser)
{
	bool pairs = false;

	if (p < end && *p == '+')
		return WORKLOAD_TAGS_MISSING;
	while (p < end) {
		const char *key, *value, *value_end;
		size_t key_length;
		char *destination = NULL;
		size_t capacity = 0;
		bool *seen = NULL;
		enum WorkloadTagStatus status;

		while (p < end && (space(*p) || *p == ','))
			p++;
		if (p == end)
			break;
		key = p;
		while (p < end && key_char(*p))
			p++;
		key_length = p - key;
		if (!key_length || p == end || *p != '=')
			return pairs ? WORKLOAD_TAGS_MALFORMED : WORKLOAD_TAGS_MISSING;
		p++;
		pairs = true;
		if (p < end && *p == '\'') {
			value = ++p;
			while (p < end && *p != '\'')
				p++;
			if (p == end)
				return WORKLOAD_TAGS_MALFORMED;
			value_end = p++;
		} else {
			value = p;
			while (p < end && bare_value_char(*p))
				p++;
			value_end = p;
			if (value == value_end)
				return WORKLOAD_TAGS_MALFORMED;
		}
		if (p < end && !space(*p) && *p != ',')
			return WORKLOAD_TAGS_MALFORMED;

		if (key_length == 4 && memcmp(key, "team", 4) == 0) {
			destination = parser->tags.team;
			capacity = sizeof(parser->tags.team);
			seen = &parser->seen_team;
		} else if (key_length == 6 && memcmp(key, "caller", 6) == 0) {
			destination = parser->tags.caller;
			capacity = sizeof(parser->tags.caller);
			seen = &parser->seen_caller;
		}
		if (!destination)
			continue;
		if (*seen)
			return WORKLOAD_TAGS_MALFORMED;
		*seen = true;
		status = decode_value(value, value_end, destination, capacity);
		if (status != WORKLOAD_TAGS_VALID)
			return status;
	}
	return WORKLOAD_TAGS_VALID;
}

enum WorkloadTagStatus workload_parse_tags(const char *sql, size_t length,
					   bool complete, struct WorkloadTags *tags)
{
	struct TagParser parser;
	const char *p = sql;
	const char *end;

	memset(tags, 0, sizeof(*tags));
	memset(&parser, 0, sizeof(parser));
	if (length > WORKLOAD_PREFIX_MAX) {
		length = WORKLOAD_PREFIX_MAX;
		complete = false;
	}
	end = sql + length;
	while (p < end) {
		const char *body, *body_end = NULL;
		size_t depth;
		bool nested = false;
		enum WorkloadTagStatus status;

		if (space(*p)) {
			p++;
			continue;
		}
		if (*p == 0)
			return WORKLOAD_TAGS_UNSUPPORTED;
		if (end - p >= 2 && p[0] == '-' && p[1] == '-') {
			while (p < end && *p != '\n')
				p++;
			continue;
		}
		if (end - p < 2) {
			if (!complete && (*p == '/' || *p == '-'))
				return WORKLOAD_TAGS_UNSUPPORTED;
			break;
		}
		if (p[0] != '/' || p[1] != '*')
			break;
		body = p + 2;
		p = body;
		depth = 1;
		while (end - p >= 2) {
			if (p[0] == '/' && p[1] == '*') {
				depth++;
				nested = true;
				p += 2;
			} else if (p[0] == '*' && p[1] == '/') {
				p += 2;
				if (--depth == 0) {
					body_end = p - 2;
					break;
				}
			} else {
				p++;
			}
		}
		if (!body_end)
			return WORKLOAD_TAGS_UNSUPPORTED;
		if (nested)
			continue;
		status = parse_comment(body, body_end, &parser);
		if (status == WORKLOAD_TAGS_MALFORMED || status == WORKLOAD_TAGS_UNSUPPORTED)
			return status;
	}
	if (p == end && !complete)
		return WORKLOAD_TAGS_UNSUPPORTED;
	*tags = parser.tags;
	return tags->team[0] && tags->caller[0] ? WORKLOAD_TAGS_VALID : WORKLOAD_TAGS_MISSING;
}

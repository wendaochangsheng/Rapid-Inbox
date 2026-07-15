#include "../src/mime_parser.h"

#include <exception>
#include <string>

namespace test {
void check(bool condition, const std::string& message);
}

namespace {

using rapid_inbox::ingestd::MimeParser;
using rapid_inbox::ingestd::ParseFailure;

rapid_inbox::ingestd::ParsedMail parse(const std::string& raw_message) {
    return MimeParser().parse(raw_message);
}

}  // namespace

void test_mime_parser_text_only_message() {
    const auto parsed = parse(
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: Hello Rapid Inbox\r\n"
        "Message-ID: <hello@example.com>\r\n"
        "Date: Wed, 13 May 2026 10:00:00 +0000\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        "Hello from C++ parser.\r\n"
        "Second line.\r\n");

    test::check(parsed.subject.value_or("") == "Hello Rapid Inbox", "subject decoded");
    test::check(parsed.from_addr.value_or("") == "sender@example.com", "from addr");
    test::check(parsed.text_body.find("Hello from C++ parser.") != std::string::npos, "text body");
    test::check(parsed.text_preview.value_or("").find("Hello from C++ parser.") == 0,
                "preview");
    test::check(parsed.headers_json.find("[[\"From\"") != std::string::npos, "headers json");
}

void test_mime_parser_html_only_message() {
    const auto parsed = parse(
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: Hello HTML\r\n"
        "Content-Type: text/html; charset=utf-8\r\n"
        "\r\n"
        "<html><body><p>Hello <strong>Rapid Inbox</strong>.</p><p>Second line</p></body></html>\r\n");

    test::check(parsed.html_body.find("<strong>Rapid Inbox</strong>") != std::string::npos,
                "html body");
    test::check(parsed.text_preview.value_or("") == "Hello Rapid Inbox. Second line",
                "html preview");
}

void test_mime_parser_multipart_alternative() {
    const auto parsed = parse(
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: Alternative\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: multipart/alternative; boundary=\"alt-boundary\"\r\n"
        "\r\n"
        "--alt-boundary\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        "Plain body from alternative.\r\n"
        "\r\n"
        "--alt-boundary\r\n"
        "Content-Type: text/html; charset=utf-8\r\n"
        "\r\n"
        "<html><body><p>Plain body from alternative.</p><p><strong>HTML</strong> body.</p></body></html>\r\n"
        "\r\n"
        "--alt-boundary--\r\n");

    test::check(parsed.text_body.find("Plain body from alternative.") != std::string::npos,
                "text body");
    test::check(parsed.html_body.find("<strong>HTML</strong> body.") != std::string::npos,
                "html body");
}

void test_mime_parser_attachment_base64() {
    const auto parsed = parse(
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: Attachment\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: multipart/mixed; boundary=\"mixed-boundary\"\r\n"
        "\r\n"
        "--mixed-boundary\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        "This message has an attachment.\r\n"
        "\r\n"
        "--mixed-boundary\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "Content-Disposition: attachment; filename=\"report.txt\"\r\n"
        "Content-Transfer-Encoding: base64\r\n"
        "\r\n"
        "UXVhcnRlcmx5IHJlcG9ydAo=\r\n"
        "\r\n"
        "--mixed-boundary--\r\n");

    test::check(parsed.attachments.size() == 1, "one attachment");
    test::check(parsed.attachments[0].filename.value_or("") == "report.txt",
                "attachment filename");
    test::check(parsed.attachments[0].content == "Quarterly report\n", "attachment content");
    test::check(parsed.attachments[0].content_type == "text/plain", "attachment content type");
}

void test_mime_parser_inline_related_part() {
    const auto parsed = parse(
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: Inline\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: multipart/related; boundary=\"related-boundary\"\r\n"
        "\r\n"
        "--related-boundary\r\n"
        "Content-Type: text/html; charset=utf-8\r\n"
        "\r\n"
        "<html><body><img src=\"cid:hero-image\" alt=\"Hero\"></body></html>\r\n"
        "\r\n"
        "--related-boundary\r\n"
        "Content-Type: image/png\r\n"
        "Content-Disposition: inline\r\n"
        "Content-ID: <hero-image>\r\n"
        "\r\n"
        "inline image bytes\r\n"
        "--related-boundary--\r\n");

    test::check(parsed.attachments.size() == 1, "one inline attachment");
    test::check(parsed.attachments[0].is_inline, "inline attachment flag");
    test::check(parsed.attachments[0].content_id.value_or("") == "<hero-image>",
                "inline content id");
    test::check(parsed.attachments[0].content == "inline image bytes\n", "inline attachment body");
}

void test_mime_parser_decodes_quoted_printable_text() {
    const auto parsed = parse(
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: QP\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "Content-Transfer-Encoding: quoted-printable\r\n"
        "\r\n"
        "Hello=2C Rapid=20Inbox!=0AThis line is soft=\r\n"
        "continued with =5Funderscore=2E\r\n");

    test::check(parsed.text_body == "Hello, Rapid Inbox!\nThis line is softcontinued with _underscore.\n",
                "quoted printable decode");
}

void test_mime_parser_decodes_encoded_subject() {
    const auto parsed = parse(
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: =?UTF-8?B?SGVsbG8g?= =?UTF-8?Q?Rapid=5FInbox?=\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        "Body\r\n");

    test::check(parsed.subject.value_or("") == "Hello Rapid_Inbox", "encoded subject");
}

void test_mime_parser_reports_malformed_multipart() {
    bool threw = false;
    try {
        (void)parse(
            "Subject: Broken\r\n"
            "Content-Type: multipart/mixed; boundary=\"missing\"\r\n"
            "\r\n"
            "body without boundary\r\n");
    } catch (const ParseFailure&) {
        threw = true;
    }
    test::check(threw, "malformed multipart throws ParseFailure");
}

void test_mime_parser_allows_boundary_trailing_whitespace() {
    const auto parsed = parse(
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: Boundary Whitespace\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: multipart/alternative; boundary=\"b\"\r\n"
        "\r\n"
        "--b   \t\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        "Plain body with boundary whitespace.\r\n"
        "--b--  \t\r\n");

    test::check(parsed.text_body.find("Plain body with boundary whitespace.") != std::string::npos,
                "boundary trailing whitespace accepted");
}

void test_mime_parser_decodes_latin1_text_body() {
    const auto parsed = parse(
        std::string(
            "From: QA Sender <sender@example.com>\r\n"
            "To: foo@adb.com\r\n"
            "Subject: Latin1\r\n"
            "Content-Type: text/plain; charset=iso-8859-1\r\n"
            "\r\n"
            "Caf") +
        std::string(1, static_cast<char>(0xe9)) + "\r\n");

    test::check(parsed.text_body.find("Caf\xc3\xa9") != std::string::npos,
                "latin1 text body converted to utf-8");
}

void test_mime_parser_truncates_preview_at_utf8_boundary() {
    const std::string headers =
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: UTF-8 preview boundary\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n";

    const auto split_code_point = parse(
        headers + std::string(198, 'a') + std::string("\xe4\xb8\xad") + "tail\r\n");
    test::check(split_code_point.text_preview.value_or("") == std::string(198, 'a'),
                "preview does not split a utf-8 code point");
    test::check(split_code_point.text_preview.value_or("").size() <= 200,
                "utf-8 preview stays within byte limit");

    const std::string exact_preview = std::string(197, 'a') + std::string("\xe4\xb8\xad");
    const auto exact_boundary = parse(headers + exact_preview + "tail\r\n");
    test::check(exact_boundary.text_preview.value_or("") == exact_preview,
                "complete utf-8 code point is retained at byte limit");
    test::check(exact_boundary.text_preview.value_or("").size() == 200,
                "utf-8 preview can fill byte limit exactly");
}

void test_mime_parser_replaces_invalid_utf8_in_preview() {
    std::string raw_message =
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: Invalid UTF-8 preview\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        "before";
    raw_message.push_back(static_cast<char>(0xff));
    raw_message += "after\r\n";

    const auto parsed = parse(raw_message);
    test::check(parsed.text_preview.value_or("") == "before\xef\xbf\xbd" "after",
                "invalid utf-8 preview bytes are replaced");
    test::check(parsed.text_preview.value_or("").size() <= 200,
                "sanitized preview stays within byte limit");
}

void test_mime_parser_keeps_empty_attachment() {
    const auto parsed = parse(
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: Empty Attachment\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: multipart/mixed; boundary=\"mixed-boundary\"\r\n"
        "\r\n"
        "--mixed-boundary\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        "Body.\r\n"
        "--mixed-boundary\r\n"
        "Content-Type: application/octet-stream\r\n"
        "Content-Disposition: attachment; filename=\"empty.bin\"\r\n"
        "\r\n"
        "--mixed-boundary--\r\n");

    test::check(parsed.attachments.size() == 1, "empty attachment retained");
    test::check(parsed.attachments[0].filename.value_or("") == "empty.bin",
                "empty attachment filename");
    test::check(parsed.attachments[0].content.empty(), "empty attachment content");
}

void test_mime_parser_decodes_gbk_encoded_subject() {
    const auto parsed = parse(
        "From: QA Sender <sender@example.com>\r\n"
        "To: foo@adb.com\r\n"
        "Subject: =?GBK?B?0enWpMLr?=\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        "Body\r\n");

    test::check(parsed.subject.value_or("") == "验证码", "gbk encoded subject");
}

void test_mime_parser_failure_is_std_exception() {
    bool caught_std = false;
    try {
        (void)parse(
            "Subject: Broken\r\n"
            "Content-Type: multipart/mixed; boundary=\"missing\"\r\n"
            "\r\n"
            "body without boundary\r\n");
    } catch (const std::exception& exc) {
        caught_std = std::string(exc.what()).find("invalid multipart boundary") != std::string::npos;
    }
    test::check(caught_std, "ParseFailure is catchable as std::exception");
}

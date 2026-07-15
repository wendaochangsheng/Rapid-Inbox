#pragma once

#include <string>

namespace rapid_inbox::ingestd {

struct DateParts {
    std::string year;
    std::string month;
    std::string day;
};

std::string utc_now();
std::string utc_add_days(const std::string& timestamp, int days);
DateParts path_date_parts(const std::string& timestamp);

}

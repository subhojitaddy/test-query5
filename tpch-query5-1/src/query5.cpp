#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <iomanip>

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--r_name") {
            if (i + 1 < argc) r_name = argv[++i];
            else return false;
        } else if (arg == "--start_date") {
            if (i + 1 < argc) start_date = argv[++i];
            else return false;
        } else if (arg == "--end_date") {
            if (i + 1 < argc) end_date = argv[++i];
            else return false;
        } else if (arg == "--threads") {
            if (i + 1 < argc) num_threads = std::stoi(argv[++i]);
            else return false;
        } else if (arg == "--table_path") {
            if (i + 1 < argc) table_path = argv[++i];
            else return false;
        } else if (arg == "--result_path") {
            if (i + 1 < argc) result_path = argv[++i];
            else return false;
        }
    }
    return !r_name.empty() && !start_date.empty() && !end_date.empty() && num_threads > 0 && !table_path.empty() && !result_path.empty();
}

// Helper to split string by delimiter
std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, 
                  std::vector<std::map<std::string, std::string>>& customer_data, 
                  std::vector<std::map<std::string, std::string>>& orders_data, 
                  std::vector<std::map<std::string, std::string>>& lineitem_data, 
                  std::vector<std::map<std::string, std::string>>& supplier_data, 
                  std::vector<std::map<std::string, std::string>>& nation_data, 
                  std::vector<std::map<std::string, std::string>>& region_data) {

    auto readTable = [&](const std::string& filename, const std::vector<std::string>& headers, std::vector<std::map<std::string, std::string>>& data) -> bool {
        std::ifstream file(table_path + "/" + filename);
        if (!file.is_open()) {
            std::cerr << "Error opening file: " << table_path + "/" + filename << std::endl;
            return false;
        }
        std::string line;
        while (std::getline(file, line)) {
            if (line.empty()) continue;
            std::vector<std::string> values = split(line, '|');
            if (values.size() < headers.size()) continue; 
            std::map<std::string, std::string> row;
            for (size_t i = 0; i < headers.size(); ++i) {
                row[headers[i]] = values[i];
            }
            data.push_back(row);
        }
        return true;
    };

    // Headers mapped roughly to TPC-H schema positions we care about, or full schema
    // Assuming schema is standard TPC-H. 
    // We only need specific columns for query 5, but the signature implies reading everything or at least populating maps.
    
    // CUSTOMER
    if (!readTable("customer.tbl", {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"}, customer_data)) return false;
    
    // ORDERS
    if (!readTable("orders.tbl", {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"}, orders_data)) return false;

    // LINEITEM
    if (!readTable("lineitem.tbl", {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"}, lineitem_data)) return false;

    // SUPPLIER
    if (!readTable("supplier.tbl", {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"}, supplier_data)) return false;

    // NATION
    if (!readTable("nation.tbl", {"n_nationkey", "n_name", "n_regionkey", "n_comment"}, nation_data)) return false;

    // REGION
    if (!readTable("region.tbl", {"r_regionkey", "r_name", "r_comment"}, region_data)) return false;

    return true;
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, 
                   const std::vector<std::map<std::string, std::string>>& customer_data, 
                   const std::vector<std::map<std::string, std::string>>& orders_data, 
                   const std::vector<std::map<std::string, std::string>>& lineitem_data, 
                   const std::vector<std::map<std::string, std::string>>& supplier_data, 
                   const std::vector<std::map<std::string, std::string>>& nation_data, 
                   const std::vector<std::map<std::string, std::string>>& region_data, 
                   std::map<std::string, double>& results) {

    // 1. Filter Regions
    std::unordered_map<std::string, std::string> region_keys; // r_regionkey -> r_name
    for (const auto& r : region_data) {
        if (r.at("r_name") == r_name) {
            region_keys[r.at("r_regionkey")] = r.at("r_name");
        }
    }

    if (region_keys.empty()) return true;

    // 2. Filter Nations in those regions
    std::unordered_map<std::string, std::string> nation_keys; // n_nationkey -> n_name
    for (const auto& n : nation_data) {
        if (region_keys.count(n.at("n_regionkey"))) {
            nation_keys[n.at("n_nationkey")] = n.at("n_name");
        }
    }

    if (nation_keys.empty()) return true;

    // 3. Filter Suppliers in those nations
    std::unordered_map<std::string, std::string> supplier_nation_map; // s_suppkey -> s_nationkey
    for (const auto& s : supplier_data) {
        if (nation_keys.count(s.at("s_nationkey"))) {
            supplier_nation_map[s.at("s_suppkey")] = s.at("s_nationkey");
        }
    }

    // 4. Filter Customers in those nations
    // Note: c_nationkey = s_nationkey is a join condition later, but efficient lookup helps
    std::unordered_map<std::string, std::string> customer_nation_map; // c_custkey -> c_nationkey
    for (const auto& c : customer_data) {
        if (nation_keys.count(c.at("c_nationkey"))) {
            customer_nation_map[c.at("c_custkey")] = c.at("c_nationkey");
        }
    }

    // 5. Filter Orders by Date and relevant Customers
    std::unordered_map<std::string, std::string> valid_orders; // o_orderkey -> o_custkey
    for (const auto& o : orders_data) {
        const std::string& o_date = o.at("o_orderdate");
        if (o_date >= start_date && o_date < end_date) {
            std::string cust_key = o.at("o_custkey");
            if (customer_nation_map.count(cust_key)) {
                valid_orders[o.at("o_orderkey")] = cust_key;
            }
        }
    }

    // 6. Process Lineitems (Multithreaded)
    std::vector<std::thread> threads;
    std::vector<std::map<std::string, double>> thread_results(num_threads);
    size_t chunk_size = (lineitem_data.size() + num_threads - 1) / num_threads;

    auto worker = [&](int thread_id, size_t start_idx, size_t end_idx) {
        for (size_t i = start_idx; i < end_idx; ++i) {
            const auto& l = lineitem_data[i];
            std::string order_key = l.at("l_orderkey");
            
            // Check if order is valid
            auto o_it = valid_orders.find(order_key);
            if (o_it != valid_orders.end()) {
                std::string supp_key = l.at("l_suppkey");
                
                // Check if supplier is valid
                auto s_it = supplier_nation_map.find(supp_key);
                if (s_it != supplier_nation_map.end()) {
                    std::string cust_key = o_it->second;
                    std::string cust_nation = customer_nation_map.at(cust_key);
                    std::string supp_nation = s_it->second;
                    
                    // Check c_nationkey = s_nationkey
                    if (cust_nation == supp_nation) {
                        double extended_price = std::stod(l.at("l_extendedprice"));
                        double discount = std::stod(l.at("l_discount"));
                        double revenue = extended_price * (1.0 - discount);
                        
                        std::string nation_name = nation_keys.at(cust_nation);
                        thread_results[thread_id][nation_name] += revenue;
                    }
                }
            }
        }
    };

    for (int i = 0; i < num_threads; ++i) {
        size_t start = i * chunk_size;
        size_t end = std::min(start + chunk_size, lineitem_data.size());
        if (start < end) {
            threads.emplace_back(worker, i, start, end);
        }
    }

    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    // 7. Aggegate Results
    for (const auto& tr : thread_results) {
        for (const auto& pair : tr) {
            results[pair.first] += pair.second;
        }
    }

    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream outfile(result_path);
    if (!outfile.is_open()) {
        std::cerr << "Error opening output file: " << result_path << std::endl;
        return false;
    }

    // Copy map to vector for sorting
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    
    // Sort by revenue descending
    std::sort(sorted_results.begin(), sorted_results.end(), 
        [](const std::pair<std::string, double>& a, const std::pair<std::string, double>& b) {
            return a.second > b.second;
        });

    for (const auto& pair : sorted_results) {
        outfile << pair.first << " " << std::fixed << std::setprecision(4) << pair.second << std::endl;
    }
    
    return true;
} 
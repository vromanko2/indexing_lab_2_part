#include <iostream>
#include <map>
#include <vector>
#include <thread>
#include <fstream>
#include <boost/filesystem.hpp>
#include <boost/locale.hpp>
#include <boost/algorithm/string.hpp>
#include <sstream>
#include <numeric>
#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <limits>
#include <chrono>
#include <zip.h>

inline std::chrono::steady_clock::time_point get_current_time_fenced() {
    assert(std::chrono::steady_clock::is_steady &&
                   "Timer should be steady (monotonic).");
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto res_time = std::chrono::steady_clock::now();
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return res_time;
}

template<class D>
inline long long to_us(const D& d)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}

template<typename T>
class threadsafe_queue {
private:
    mutable std::mutex mut;
    std::queue<T> data_queue;
    std::condition_variable data_cond;
    size_t queue_size = 0;
    size_t QUEUE_MAX_SIZE = 107374182400; //1 Gb
public:
    threadsafe_queue(){};
    threadsafe_queue(threadsafe_queue const&);
    void push(T);
    void wait_and_pop(T&);
    std::shared_ptr<T> wait_and_pop();
    bool try_pop(T&);
    std::shared_ptr<T> try_pop();
    bool empty() const;
    bool try_pop_2(T& , T&);
    const size_t size();
    bool try_push(T);
    void wait_and_push(T);
    bool can_push(size_t);
};

template<typename T>
threadsafe_queue<T>::threadsafe_queue(threadsafe_queue const& other)
{
    std::lock_guard<std::mutex> lk(other.mut);
    data_queue=other.data_queue;
}

template<typename T>
void threadsafe_queue<T>::push(T new_value)
{
    std::lock_guard<std::mutex> lk(mut);
    data_queue.push(new_value);
    data_cond.notify_one();
}

template<typename T>
void threadsafe_queue<T>::wait_and_pop(T& value)
{
    std::unique_lock<std::mutex> lk(mut);
    data_cond.wait(lk,[this]{return !data_queue.empty();});
    value=data_queue.front();
    data_queue.pop();
}

template<typename T>
std::shared_ptr<T> threadsafe_queue<T>::wait_and_pop()
{
    std::unique_lock<std::mutex> lk(mut);
    data_cond.wait(lk,[this]{return !data_queue.empty();});
    std::shared_ptr<T> res(std::make_shared<T>(data_queue.front()));
    data_queue.pop();
    return res;
}


template<typename T>
bool threadsafe_queue<T>::try_pop(T& value)
{
    std::lock_guard<std::mutex> lk(mut);
    if(data_queue.empty())
        return false;
    value=data_queue.front();
    data_queue.pop();

    size_t pop_size;
    try{
        pop_size = value.size();
    } catch (...){
        pop_size = sizeof(value);
    }

    queue_size -= pop_size;

    data_cond.notify_all();
    return true;
}

template<typename T>
bool threadsafe_queue<T>::try_pop_2(T& value_1, T& value_2)
{
    std::lock_guard<std::mutex> lk(mut);
    if(data_queue.size() < 2)
        return false;

    value_1 =data_queue.front();
    data_queue.pop();

    value_2 =data_queue.front();
    data_queue.pop();
    return true;
}

template<typename T>
std::shared_ptr<T> threadsafe_queue<T>::try_pop()
{
    std::lock_guard<std::mutex> lk(mut);
    if(data_queue.empty())
        return std::shared_ptr<T>();
    std::shared_ptr<T> res(std::make_shared<T>(data_queue.front()));
    data_queue.pop();
    data_cond.notify_all();
    return res;
}

template<typename T>
bool threadsafe_queue<T>::empty() const
{
    std::lock_guard<std::mutex> lk(mut);
    return data_queue.empty();
}

template<typename T>
const size_t threadsafe_queue<T>::size() {
    std :: lock_guard <std :: mutex> lock (this->mut);
    return data_queue.size();
}

template<typename T>
bool threadsafe_queue<T>::try_push(T new_value)
{
    std::lock_guard<std::mutex> lk(mut);
    size_t push_size;
    try{
        push_size = new_value.size();
    } catch (...){
        push_size = sizeof(new_value);
    }
    if (queue_size + push_size <= QUEUE_MAX_SIZE){
        data_queue.push(new_value);
        queue_size += push_size;
        data_cond.notify_one();
        return true;
    } else {
        data_cond.notify_one();
        return false;
    }
}

template<typename T>
void threadsafe_queue<T>::wait_and_push(T new_value) {
    std::unique_lock<std::mutex> lk(mut);
    size_t push_size;
    try{
        push_size = new_value.size();
    } catch (...){
        push_size = sizeof(new_value);
    }
    if(!can_push(push_size))
        std::cout << "Waiting" << std::endl;
    data_cond.wait(lk,[this, push_size] {return can_push(push_size);});
    data_queue.push(new_value);
    queue_size += push_size;
}

template<typename T>
bool threadsafe_queue<T>::can_push(size_t size) {
    //std::cout << QUEUE_MAX_SIZE - queue_size << std::endl;
    return queue_size + size <= QUEUE_MAX_SIZE;
}

struct configuration_t
{
    std::string indir, out_by_a, out_by_n;
    size_t indexing_threads, merging_threads;
};

struct recursive_directory_range
{
    typedef boost::filesystem::recursive_directory_iterator iterator;
    recursive_directory_range(boost::filesystem::path p) : p_(p) {}

    iterator begin() {
        return boost::filesystem::recursive_directory_iterator(p_);
    }
    iterator end() {
        return boost::filesystem::recursive_directory_iterator();
    }

    boost::filesystem::path p_;
};


class main_task {
  private:
    std::string path_entry;   //directory where scannig starts

    size_t scanning_threads_count = 1;
    size_t reading_threads_count = 1;
    size_t indexing_threads_count;
    size_t merging_threads_count;

    std::atomic_int scanning_threads_working;
    std::atomic_int reading_threads_working;
    std::atomic_int indexing_threads_working;
    std::atomic_int merging_threads_working;


    std::string out_by_a;
    std::string out_by_n;

    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;

    threadsafe_queue<std::string> queue_filepath;
    threadsafe_queue<std::string> queue_texts;
    threadsafe_queue<std::map <std::string, int>> queue_indexes;

    std::condition_variable cv_indexes;

    std::vector<std::thread> threads;

    void read_configuration(const std::string conf_file_name);

    void scanning_routine();
    void reading_into_memory_routine();
    void indexing_routine();
    void merging_routine();


  public:

    main_task(const std::string);
    void do_main_routine();
    void writing_results();
    std::chrono::steady_clock::duration result_time(){
        return end_time - start_time;
    }
};


void main_task::read_configuration(const std::string conf_file_name) {
    std::ifstream cf(conf_file_name);
    if(!cf.is_open()) {
        std::cerr << "Failed to open configuration file " << conf_file_name << std::endl;
        return;
    }

    std::ios::fmtflags flags( cf.flags() );
    cf.exceptions(std::ifstream::failbit);

    configuration_t res;
    std::string temp;

    try {
        cf >> res.indir;
        getline(cf, temp);
        cf >> res.out_by_a;
        getline(cf, temp);
        cf >> res.out_by_n;
        getline(cf, temp);
        cf >> res.indexing_threads;
        getline(cf, temp);
        cf >> res.merging_threads;
        getline(cf, temp);


    }catch(std::ios_base::failure &fail)
    {
        cf.flags( flags );
        throw;
    }
    cf.flags( flags );
    if( res.indexing_threads < 1 || res.merging_threads < 1) {
        throw std::runtime_error("The number of threads should be at least 1");
    }

    path_entry = res.indir;
    indexing_threads_count = res.indexing_threads;
    merging_threads_count = res.merging_threads;
    out_by_a = res.out_by_a;
    out_by_n = res.out_by_n;

}


void main_task::scanning_routine(){
    scanning_threads_working ++;
    for (auto it : recursive_directory_range(path_entry))
    {
        if(is_regular_file(it)){
            std :: string extension = boost::filesystem::extension(it);
            if ((extension == ".txt") || (extension == ".zip") || (extension == ".TXT") || (extension == ".ZIP")) {
                queue_filepath.push((it.path()).string());
            }
        }
    }
    scanning_threads_working--;
}


void main_task::reading_into_memory_routine(){
    reading_threads_working ++;


    std::string file_name;
    std::string block;
    std::stringstream ss;

    while(scanning_threads_working || !queue_filepath.empty()) {
        if(queue_filepath.try_pop(file_name)) {
            auto file_extension = boost::filesystem::extension(file_name);

            if (file_extension != ".txt" && file_extension != ".TXT") {
                struct zip *zip_file; // zip file descriptor
                struct zip_file *file_in_zip; // file decriptor inside archive
                struct zip_stat file_info;
                int err; // error code
                int files_total; // count of files in archive
                int file_number;
                int r;
                char buffer[10000];

                zip_file = zip_open(file_name.c_str(), 0, &err);
                if (!zip_file) {
                    std::cout << "Error: can't open file " << file_name.c_str() << std::endl;
                    continue;
                };


                files_total = zip_get_num_files(zip_file);

                for (int file_number = 0; file_number < files_total; file_number++) {
                    zip_stat_index(zip_file, file_number, 0, &file_info);

                    if(boost::filesystem::extension(file_info.name) != ".txt" && boost::filesystem::extension(file_info.name) != ".TXT")
                        continue;


                    file_in_zip = zip_fopen_index(zip_file, file_number, 0);

                    if(file_in_zip) {
                        std::string file_content = "";

                        while ( (r = zip_fread(file_in_zip, buffer, sizeof(buffer))) > 0) {
                            file_content.append(buffer, r);
                        };
                        zip_fclose(file_in_zip);
                        queue_texts.wait_and_push(file_content);
                    } else {
                        std::cout << "     Error: can't open file in zip " << file_info.name << std::endl;
                    };
                };

                zip_close(zip_file);
            } else if (file_extension == ".txt" || file_extension == ".TXT"){

                std::ifstream file (file_name); //method of reading the file from cms link
                auto const start_pos = file.tellg();
                file.ignore(std::numeric_limits<std::streamsize>::max());
                auto const char_count = file.gcount();
                file.seekg(start_pos);
                auto s = std::vector<char>((unsigned long)char_count);
                (file).read(&s[0], s.size());
                file.close();

                queue_texts.wait_and_push(std::string(s.begin(), s.end()));
            }
        }
    }
    reading_threads_working --;
}

void main_task::indexing_routine()
{
    std::string txt;
    std::vector <std::string> vect_words;
    std::map <std::string, int> map_words;
    std::string str;

    indexing_threads_working++;
    boost::locale::generator gen;
    std::locale loc=gen("");
    std::locale::global(loc);
    std::cout.imbue(loc);
    while(reading_threads_working || !queue_texts.empty())
    {
        if(queue_texts.try_pop(std::ref(txt))) {
            //boost::split(vect_words, txt, boost::is_any_of(" ,.!?"), boost::token_compress_on);
            boost::locale::boundary::ssegment_index map(boost::locale::boundary::word, txt.begin(), txt.end());
            map.rule(boost::locale::boundary::word_any);
            for (boost::locale::boundary::ssegment_index::iterator it = map.begin(), e = map.end(); it != e; ++it) {
                str = *it;
                vect_words.emplace_back(boost::locale::fold_case(boost::locale::normalize(str)));
                str = "";
            }
            for (int i = 0; i < vect_words.size(); ++i)
            {
                ++map_words[vect_words[i]];
            }
            queue_indexes.push(map_words);

            vect_words.clear();
            map_words.clear();

        }

        txt = "";
    }

    indexing_threads_working--;
}

void main_task::merging_routine() {
   merging_threads_working ++;

   std :: map <std :: string, int> map1, map2, result_map;
   std :: pair <std::string, int> word;

    while(true) {
        if (queue_indexes.try_pop(map1)){
            if(queue_indexes.try_pop(map2)) {
                for (auto &word : map2)
                    map1[word.first] += word.second;
                queue_indexes.push(map1);
            } else {
                queue_indexes.push(map1);
                if (!indexing_threads_working || merging_threads_working > 1)
                    break;
            }
            std::cout << queue_texts.size() << " : " << queue_indexes.size() << std::endl;
        }
    }

   merging_threads_working --;
}

void PrintInFile(std :: fstream & file, const std::vector <std::pair < std::string, int>> & vect){
    if(!file.is_open()){
        std :: cout << "Problem with file" << std :: endl;
        return;
    }
    for (int i = 0; i < vect.size(); i ++) {
        file << vect[i].first << " : " << vect[i].second << std::endl;
    }
}

void main_task::writing_results() {



    std::vector <std::pair<std::string, int>> v;
    std::map <std::string, int> result;

    std :: fstream fs_out_by_a (out_by_a);
    std :: fstream fs_out_by_n (out_by_n);


    std::map <std::string, int> temp;
    if(queue_indexes.size() == 1){
        queue_indexes.try_pop(result);
        copy(result.begin(), result.end(), back_inserter(v));

        PrintInFile(fs_out_by_a, v);
        sort(v.begin(), v.end(), [] (const std::pair <std::string, int> & a, const std::pair <std::string, int> & b){
            return (a.second > b.second);
        });
        PrintInFile(fs_out_by_n, v);
    } else {
        std::cout << "You are looser" << std::endl;
        return;
    }
    std::cout << to_us(result_time() / 1000000 ) << " s" << std::endl;
}

main_task::main_task(const std::string conf_file_name)
{
    read_configuration(conf_file_name);
}

void main_task::do_main_routine()
{
    start_time = get_current_time_fenced();
    threads.emplace_back(&main_task::scanning_routine, this);



    for(int i=0; i < merging_threads_count; i++)
        threads.emplace_back(&main_task::merging_routine, this);

    threads.emplace_back(&main_task::reading_into_memory_routine, this);

    for(int i=0; i < indexing_threads_count; i++)
        threads.emplace_back(&main_task::indexing_routine, this);


    for (auto & th : threads)
        th.join();
    end_time = get_current_time_fenced();

}


int main(int argc, char* argv[])
{

  std::string conf_file_name ("conf.txt");
    if(argc == 2)
        conf_file_name = argv[1];
    if(argc > 2) {
        std::cerr << "Too many arguments. Usage: \n"
                     "<program>\n"
                     "or\n"
                     "<program> <config-filename>\n" << std::endl;
        return 1;
    }

  main_task tsk(conf_file_name);

  tsk.do_main_routine();
  tsk.writing_results();
}

#include <iostream>
#include <memory>
#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <atomic>
#include <fstream>


template <typename T>
class Subscriber;

template <typename T>
class Topic;


template <typename T>
class Publisher
{
public:
    Publisher() = default;
    Publisher(Topic<T>* topic) : _topic{topic}
    {}

    void produce(Topic<T>& t, const T& msg)
    {
        t.send(msg);
    }

    void produce(const T& msg)
    {
        if(_topic != nullptr)
            _topic->send(msg);
    }

private:
    Topic<T>* _topic{nullptr};
};


template <typename T = std::string>
class Topic
{
public:
    using msg_type = T;

    Topic() = default;
    Topic(std::string name) : _name{std::move(name)}
    {}

    void send(const msg_type& data)
    {
        for(auto& sub : subs)
            sub->deliver(_name, data);
    }

    void subscribe(Subscriber<T>* s)
    {
        subs.push_back(s);
    }

    Publisher<T> create_publisher()
    {
        return Publisher<T>(this);
    }

private:
    std::string _name{};
    std::vector<Subscriber<T>*> subs;
};


template <typename T>
class Subscriber
{
public:
    using f_callback_t = std::function<void(const std::string& topic_name, T data)>;

    Subscriber(f_callback_t f) : callaback{std::move(f)}
    {}

    Subscriber(Topic<T>& topic, f_callback_t f) : callaback{std::move(f)}
    {
        subscribe(topic);
    }

    void subscribe(Topic<T>& topic)
    {
        topic.subscribe(this);
    }

    void deliver(const std::string& topic_name, const T& msg)
    {
        std::lock_guard<std::mutex> l(m);
        data.push(std::make_pair(topic_name, msg));
    }

    void run()
    {
        stopped = false;
        th = std::thread([this](){
            while (true)
            {
                std::unique_lock<std::mutex> g(m);
                const auto is_empty = data.empty();
                if (is_empty && stopped)
                    break;

                if (!is_empty)
                {
                    auto msg = data.front();
                    data.pop();
                    g.unlock();
                    callaback(msg.first, msg.second);
                }
                else
                {
                    g.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(3));
                }
            }
        });
    }

    void stop()
    {
        if (!stopped)
        {
            stopped = true;
            th.join();
        }
    }

    ~Subscriber()
    {
        stop();
    }

private:

    std::function<void(const std::string& topic_name, const T& data)> callaback;
    std::queue<std::pair<std::string, T>> data;
    std::atomic<bool> stopped{true};
    std::mutex m;
    std::thread th;
};


int main()
{
    Topic<std::string> meteo("meteo");
    auto meteo_station = meteo.create_publisher();

    size_t web_news_counter{};
    Subscriber<std::string> web_news(meteo, [&web_news_counter](const std::string& topic_name, const std::string& data){
        web_news_counter++;
    });

    size_t ansa_counter{};
    Subscriber<std::string> ansa(meteo, [&ansa_counter](const std::string& topic_name, const std::string& data){
        ansa_counter++;
    });


    web_news.run();
    ansa.run();

    std::thread th_meteo([&]{
        std::vector<std::string> cities{"Rome", "Florence", "Venice"};

        for (uint32_t i = 0; i < 1000000; i++)
            meteo_station.produce(cities[i%3] + ", " + std::to_string(24 + (i%10)));
    });

    th_meteo.join();

    web_news.stop();
    ansa.stop();

    std::cout << "web_news_counter: " << web_news_counter << std::endl;
    std::cout << "ansa_counter: " << ansa_counter << std::endl;

    return 0;
}

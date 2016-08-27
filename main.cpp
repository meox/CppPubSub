#include <iostream>
#include <fstream>
#include "pubsub.hpp"

using namespace ps;

int main()
{
    auto meteo = create_topic<std::string>("meteo");
    auto meteo_station = create_publisher(meteo);

    auto temp = create_topic<std::string>("temp");
    auto season = create_publisher(temp);

    size_t web_news_counter{};
    Subscriber<std::string> web_news(meteo, [&web_news_counter](const Topic<std::string>* topic, const std::string& data){
        web_news_counter++;
    });

    size_t ansa_counter{};
    Subscriber<std::string> ansa(meteo, [&ansa_counter](const Topic<std::string>* topic, const std::string& data){
        ansa_counter++;
    });


    size_t g_counter{};
    Subscriber<std::string> global({meteo, temp}, [&g_counter](const Topic<std::string>* topic, const std::string& data){
        g_counter++;
    });

    web_news.run();
    ansa.run();
    global.run();

    std::thread th_meteo([&]{
        std::vector<std::string> cities{"Rome", "Florence", "Venice"};

        for (uint32_t i = 0; i < 1000000; i++)
        {
            const auto temp = std::to_string(24 + (i%10));
            meteo_station->produce(cities[i%3] + ", " + temp);
            season->produce(temp);
        }
    });

    th_meteo.join();

    web_news.unsubscribe();
    ansa.unsubscribe();
    global.unsubscribe();

    std::cout << "web_news_counter: " << web_news_counter << std::endl;
    std::cout << "ansa_counter: " << ansa_counter << std::endl;
    std::cout << "g_counter: " << g_counter << std::endl;

    return 0;
}

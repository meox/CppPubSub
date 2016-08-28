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
    auto web_news = create_subscriber(meteo, [&web_news_counter](const Topic<std::string>* topic, const std::string& data){
        web_news_counter++;
    });

    size_t ansa_counter{};
    auto ansa = create_subscriber(meteo, [&ansa_counter](const Topic<std::string>* topic, const std::string& data){
        ansa_counter++;
    });


    size_t g_counter{};
    auto global = create_subscriber({meteo, temp}, [&g_counter](const Topic<std::string>* topic, const std::string& data){
        g_counter++;
    });

    web_news->run();
    ansa->run();
    global->run();

    std::thread th_meteo([&]{
        std::vector<std::string> cities{"Rome", "Florence", "Venice"};

        for (uint32_t i = 0; i < 1000000; i++)
        {
            const auto tcelsius = std::to_string(24 + (i%10));
            meteo_station->produce(cities[i%3] + ", " + tcelsius);
            season->produce(tcelsius);
        }
    });

    th_meteo.join();

    web_news->unsubscribe();
    ansa->unsubscribe();
    global->unsubscribe();

    std::cout << "web_news_counter: " << web_news_counter << std::endl;
    std::cout << "ansa_counter: " << ansa_counter << std::endl;
    std::cout << "g_counter: " << g_counter << std::endl;

    return 0;
}

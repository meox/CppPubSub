#include <iostream>
#include <fstream>
#include "pubsub.hpp"


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

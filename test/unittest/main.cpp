#define CATCH_CONFIG_RUNNER
#include <catch/catch.hpp>

int main(int argc, char *argv[]) {
    try
    {
	    return Catch::Session().run(argc, argv);
    }
    catch(const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    catch(...)
    {
        std::cerr << "Unknown error" << std::endl;
        return 1;
    }
}

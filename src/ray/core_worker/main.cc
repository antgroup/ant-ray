
#include <endian.h>
#include "ray/core_worker/reference_count.h"
#include "ray/common/id.h"
// #include <vector>
#include <iostream>
// #include <memory>


int main(int argc, char *argv[]){

    boost::flyweight<ray::NodeID> nodeid_pool;
    absl::optional<boost::flyweight<ray::NodeID>> optional_nodeid_pool;
    bool has_val = optional_nodeid_pool.has_value();
    std::cout<< has_val << std::endl;
    // optional_nodeid_pool.value_or(ray::NodeID::Nil()); // Error

    // boost::flyweight<ray::NodeID> nodeid_pool2 = ray::NodeID::Nil(); // Error
    boost::flyweight<ray::NodeID> nodeid_pool3;
    nodeid_pool3 = ray::NodeID::Nil(); // OK, what's the diff?

    boost::flyweight<ray::NodeID> nodeid_pool4(ray::NodeID::Nil()); // OK
    
    const absl::optional<ray::NodeID> pinned_at_raylet_id;
    // pinned_at_raylet_id = ray::NodeID::Nil(); // will be passed as a func param
    has_val = pinned_at_raylet_id.has_value();
    std::cout<< has_val << std::endl;
    std::cout<< pinned_at_raylet_id.value() << std::endl; // what():  bad optional access
    
    absl::optional<boost::flyweight<ray::NodeID>> flyweight_pinned_at_raylet_id(nodeid_pool4);

}
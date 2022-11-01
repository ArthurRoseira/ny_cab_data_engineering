function delay(time) {
    return new Promise(resolve => setTimeout(resolve, time));
}

taxi_zones_list = JSON.parse(taxi_zones_list)
var map = L.map('map').setView([40.73,-74], 10);

L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '© OpenStreetMap'
}).addTo(map);

var markers = {}


function set_layer(element,color){
    try{
        map.removeLayer(markers[element.id])
    }catch{
        
    }finally{
        console.log(element)
        map.setView([40.73,-74], 10)
        delay(1000).then(()=>{
            selected_value_index = taxi_zones_list.findIndex((zone) => zone.Zone==element.value);
            selected_value = taxi_zones_list[selected_value_index]
            console.log(selected_value)
            marker = L.marker([selected_value.coordinates.latitude,selected_value.coordinates.longitude]).addTo(map)
            markers[element.id] = marker
        })
    }
}

var destination_selection = document.getElementById('from_zone');
var origin_selection = document.getElementById('to_zone');

destination_selection.addEventListener('change', function onChange(event){
    set_layer(event.target,'green')
}, false);

origin_selection.addEventListener('change', function onChange(event){
    set_layer(event.target,'green')
}, false);


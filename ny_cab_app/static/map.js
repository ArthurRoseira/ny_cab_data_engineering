function delay(time) {
    return new Promise(resolve => setTimeout(resolve, time));
}
  

var map = L.map('map').setView([40.73,-74], 10);

L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '© OpenStreetMap'
}).addTo(map);

// var topic =document.getElementById('topic').innerHTML
// console.log('/topic/'+topic)
// var source  = new EventSource('/topic/'+topic)
// var Lines = {}

// source.addEventListener('message',function(e){

//     console.log('message')
//     data = JSON.parse(e.data);
//     console.log(data)
//     map.setView([data.latitude,data.longitude],3)
//     delay(1000).then(()=>{if (!(data.bus_line in Lines)){
//         Lines[data.bus_line] = []
//         marker = L.marker([data.latitude,data.longitude]).addTo(map)
//         Lines[data.bus_line].push(marker)
//     }else{
//         // map.removeLayer(Lines[data.bus_line][Lines[data.bus_line].lenght - 1])
//         marker = L.marker([data.latitude,data.longitude]).addTo(map)
//         Lines[data.bus_line].push(marker)
//     }})

// },false)


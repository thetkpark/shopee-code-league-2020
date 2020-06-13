const csv = require('csv-parser')
const createCsvWriter = require('csv-writer').createObjectCsvWriter
const fs = require('fs')

const data = {}
const brushedShop = []

fs.createReadStream('order_brush_order.csv')
	.pipe(csv())
	.on('data', row => {
		row.event_time = new Date(row.event_time)

		if (data[row.shopid]) {
			data[row.shopid].push(row)
		} else {
			data[row.shopid] = []
			data[row.shopid].push(row)
		}
	})
	.on('end', () => {
		for (let i in data) {
			sortByOrderTime(data[i])
			// Implementation of something after the data is parsed
			const queue = []
			const map = {}
			map.unique = 0
			data[i].forEach(ele => {
				queue.push(ele)
				while (ele.event_time.getTime() - queue[0].event_time.getTime() > 3600000) {
					map[queue[0].userid] = 0
					map.unique--
					queue.shift()
				}

				if (!map[ele.userid]) {
					map[ele.userid] = 1
					map.unique++
				}
				const rate = queue.length / map.unique
				if (rate >= 3) {
					const index = brushedShop.findIndex(element => element.shopid == ele.shopid)
					const users = findMaxId(map)
					if (index == -1) {
						brushedShop.push({
							shopid: ele.shopid,
							userid: users,
						})
					} else {
						brushedShop[index].userid += `&${users}`
					}
				}
			})
		}

		// console.log(brushedShop)

		const csvWriter = createCsvWriter({
			path: 'out.csv',
			header: [
				{ id: 'shopid', title: 'shopid' },
				{ id: 'userid', title: 'userid' },
			],
		})

		csvWriter.writeRecords(brushedShop).then(() => console.log('The CSV file was written successfully'))
	})

function sortByOrderTime(arr) {
	arr.sort((ele1, ele2) => {
		if (ele1.event_time < ele2.event_time) {
			return -1
		} else if (ele1.event_time > ele2.event_time) {
			return 1
		} else {
			return 0
		}
	})
}
// {
//     <shopID>: [{
//         order_id:
//         shopid:
//         userid:
//         event_time: order by this prop
//         },...]
// }
function findMaxId(map) {
	let max = 0
	let id = ''
	for (let i in map) {
		if (i == 'unique') continue
		if (map[i] == max) {
			id = id + `&${i}`
		} else if (map[i] > max) {
			id = i
			max = map[i]
		}
	}
	return id
}

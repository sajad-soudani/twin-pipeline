package topic

import "strings"

func IsSubTopic(topic, wanted string) bool {
	a := strings.Split(topic, "/")
	b := strings.Split(wanted, "/")
	var status bool

	if len(b) > len(a) {
		return false
	}
	for i, v := range b { // always the wanted topic is shorter than the sent
		if v == a[i] {
			status = true
		} else {
			status = false
		}
	}

	return status
}

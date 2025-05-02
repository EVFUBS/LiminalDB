package interpreter

func lessThanComparison(left interface{}, right interface{}) (bool, interface{}, error) {
	switch l := left.(type) {
	case int64:
		switch r := right.(type) {
		case int64:
			return true, l < r, nil
		case float64:
			return true, float64(l) < r, nil
		}
	case float64:
		switch r := right.(type) {
		case int64:
			return true, l < float64(r), nil
		case float64:
			return true, l < r, nil
		}
	}
	return false, nil, nil
}

func lessThanOrEqualComparison(left interface{}, right interface{}) (bool, interface{}, error) {
	switch l := left.(type) {
	case int64:
		switch r := right.(type) {
		case int64:
			return true, l <= r, nil
		case float64:
			return true, float64(l) <= r, nil
		}
	case float64:
		switch r := right.(type) {
		case int64:
			return true, l <= float64(r), nil
		case float64:
			return true, l <= r, nil
		}
	}
	return false, nil, nil
}

func greaterThanComparison(left interface{}, right interface{}) (bool, interface{}, error) {
	switch l := left.(type) {
	case int64:
		switch r := right.(type) {
		case int64:
			return true, l > r, nil
		case float64:
			return true, float64(l) > r, nil
		}
	case float64:
		switch r := right.(type) {
		case int64:
			return true, l > float64(r), nil
		case float64:
			return true, l > r, nil
		}
	}
	return false, nil, nil
}

func greaterThanOrEqualComparison(left interface{}, right interface{}) (bool, interface{}, error) {
	switch l := left.(type) {
	case int64:
		switch r := right.(type) {
		case int64:
			return true, l >= r, nil
		case float64:
			return true, float64(l) >= r, nil
		}
	case float64:
		switch r := right.(type) {
		case int64:
			return true, l >= float64(r), nil
		case float64:
			return true, l >= r, nil
		}
	}
	return false, nil, nil
}

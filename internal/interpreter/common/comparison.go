package common

func LessThanComparison(left any, right any) (bool, any, error) {
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

func LessThanOrEqualComparison(left any, right any) (bool, any, error) {
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

func GreaterThanComparison(left any, right any) (bool, any, error) {
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

func GreaterThanOrEqualComparison(left any, right any) (bool, any, error) {
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

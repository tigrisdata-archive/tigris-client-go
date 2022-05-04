package update

func ExampleUpdate() {
	u := Set("field_1", "aaa").
		Set("field_2", 123).
		Unset("field3")

	drvUpdate, err := u.Build()
	if err != nil {
		panic(err)
	}

	_ = drvUpdate
}

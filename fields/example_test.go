package fields

func ExampleUpdate() {
	// Reusable update fields
	u := Set("field_1", "aaa").
		Set("field_2", 123).
		Unset("field3")

	update, err := u.Build()
	if err != nil {
		panic(err)
	}

	// Now update can be passed to Update call
	_ = update
}

func ExampleRead() {
	// Reusable read fields
	r := Include("field_1").
		Exclude("field_2")

	read, err := r.Build()
	if err != nil {
		panic(err)
	}

	// Now read can be passed to Read calls
	_ = read
}

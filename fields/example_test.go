// Copyright 2022-2023 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

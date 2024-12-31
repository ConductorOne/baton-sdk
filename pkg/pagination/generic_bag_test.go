package pagination

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenBagMarshalling(t *testing.T) {
	type innerStruct struct {
		Age  int
		Name string
	}

	var bag GenBag[innerStruct]

	bag.Push(innerStruct{Age: 10, Name: "John"})
	bag.Push(innerStruct{Age: 20, Name: "Doe"})

	// Marshal
	marshalled, err := bag.Marshal()
	require.NoError(t, err)

	// Unmarshal
	err = bag.Unmarshal(marshalled)
	require.NoError(t, err)
}

func TestGenBagFromToken(t *testing.T) {
	type innerStruct struct {
		Age  int
		Name string
	}

	var bag GenBag[innerStruct]

	bag.Push(innerStruct{Age: 10, Name: "John"})
	bag.Push(innerStruct{Age: 20, Name: "Doe"})

	// Marshal
	marshalled, err := bag.Marshal()
	require.NoError(t, err)

	token := &Token{Token: marshalled}

	// Unmarshal
	bagFromToken, err := GenBagFromToken[innerStruct](token)
	require.NoError(t, err)

	require.Equal(t, &bag, bagFromToken)
}

func TestGenBagUnmarshal(t *testing.T) {
	type innerStruct struct {
		Age  int
		Name string
	}

	var bag GenBag[innerStruct]

	err := bag.Unmarshal("{invalid")
	require.Error(t, err)

	err = bag.Unmarshal("")

	require.NoError(t, err)
	require.Nil(t, bag.states)
	require.Nil(t, bag.currentState)

	marshal, err := bag.Marshal()
	require.NoError(t, err)
	require.Equal(t, "", marshal)

	temp := bag.Current()
	require.Nil(t, temp)

	{
		compare := innerStruct{Age: 10, Name: "John"}

		bag.Push(compare)

		current := bag.Current()
		require.Equal(t, &compare, current)
	}

	{
		first := innerStruct{Age: 10, Name: "John"}
		second := innerStruct{Age: 15, Name: "Wick"}

		bag.Push(first)
		bag.Push(second)

		current := bag.Current()
		require.Equal(t, &second, current)
	}

	{
		first := innerStruct{Age: 10, Name: "John"}
		second := innerStruct{Age: 15, Name: "Wick"}

		bag.Push(first)
		bag.Push(second)
		pop := bag.Pop()
		require.Equal(t, pop, &second)

		current := bag.Current()
		require.Equal(t, &first, current)
	}
}

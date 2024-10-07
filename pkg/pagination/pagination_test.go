package pagination

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPaginationBagMarshalling(t *testing.T) {
	bag := &Bag{}
	groupResourceType := "group"
	nextPageToken := "page=1"
	firstPageState := PageState{
		ResourceTypeID: groupResourceType,
	}

	bag.push(firstPageState)
	err := bag.Next(nextPageToken)
	require.NoError(t, err)
	nextPage, err := bag.Marshal()
	require.NoError(t, err)
	err = bag.Unmarshal(nextPage)
	require.NoError(t, err)
	require.Equal(t, bag.PageToken(), nextPageToken)
	require.Equal(t, bag.currentState.ResourceTypeID, groupResourceType)
	require.NotEqual(t, bag.Current(), firstPageState)
	firstPageState.Token = nextPageToken
	require.Equal(t, *bag.Current(), firstPageState)
}

func comparePageState(t *testing.T, expected PageState, actual PageState) {
	require.NotNil(t, actual)
	require.Equal(t, expected, actual)
	require.Equal(t, expected.Token, actual.Token)
	require.Equal(t, expected.ResourceID, actual.ResourceID)
	require.Equal(t, expected.ResourceTypeID, actual.ResourceTypeID)
}

func TestPaginationBag(t *testing.T) {
	pb := &Bag{}
	ps1 := PageState{Token: ""}
	ps2 := PageState{Token: "", ResourceTypeID: "user", ResourceID: "userID1"}
	ps3 := PageState{Token: "1234", ResourceTypeID: "repo", ResourceID: "repo42"}
	ps4 := PageState{Token: "5678", ResourceTypeID: "repo", ResourceID: "repo42"}

	pb.push(ps1)
	comparePageState(t, ps1, *pb.Current())
	require.Empty(t, pb.states)
	pb.push(ps2)
	comparePageState(t, ps2, *pb.Current())
	require.Len(t, pb.states, 1)
	comparePageState(t, ps1, pb.states[0])

	popped := pb.pop()
	comparePageState(t, ps2, *popped)
	comparePageState(t, ps1, *pb.Current())
	require.Len(t, pb.states, 0)

	pb.push(ps3)
	comparePageState(t, ps3, *pb.Current())
	require.Len(t, pb.states, 1)
	comparePageState(t, ps1, pb.states[0])

	pb.push(ps4)
	comparePageState(t, ps4, *pb.Current())
	require.Len(t, pb.states, 2)
	comparePageState(t, ps1, pb.states[0])
	comparePageState(t, ps3, pb.states[1])

	popped = pb.pop()
	comparePageState(t, ps4, *popped)
	comparePageState(t, ps3, *pb.Current())
	require.Len(t, pb.states, 1)
	comparePageState(t, ps1, pb.states[0])

	popped = pb.pop()
	comparePageState(t, ps3, *popped)
	comparePageState(t, ps1, *pb.Current())
	require.Len(t, pb.states, 0)

	popped = pb.pop()
	comparePageState(t, ps1, *popped)
	require.Nil(t, pb.Current())
	require.Len(t, pb.states, 0)
}

func TestPageBagMarshalUnmarshal(t *testing.T) {
	st := &Bag{}
	states := []PageState{
		{Token: ""},
		{Token: "", ResourceTypeID: "user", ResourceID: "userID1"},
		{Token: "1234", ResourceTypeID: "repo", ResourceID: "repo42"},
		{Token: "5678", ResourceTypeID: "repo", ResourceID: "repo42"},
	}

	for _, s := range states {
		st.push(s)
	}

	tokenString, err := st.Marshal()
	require.NoError(t, err)

	newBag := &Bag{}
	err = newBag.Unmarshal(tokenString)
	require.NoError(t, err)

	i := len(states) - 1
	for newBag.Current() != nil {
		comparePageState(t, states[i], *newBag.pop())
		i--
	}

	require.Equal(t, i, -1)
}

func TestPaginationTokenUnmarshalEmptyString(t *testing.T) {
	pb := &Bag{}
	pt := "page=1"
	ps := PageState{Token: pt}
	err := pb.Unmarshal("")
	require.NoError(t, err)

	pb.push(ps)
	comparePageState(t, ps, *pb.Current())
}

func TestPaginationBagNextPage(t *testing.T) {
	bag := &Bag{}
	ps1 := PageState{Token: "page=1"}
	ps2 := PageState{Token: "page=2"}

	bag.Push(ps1)
	comparePageState(t, ps1, *bag.Current())
	require.Len(t, bag.states, 0)

	_, err := bag.NextToken("page=2")
	require.NoError(t, err)
	require.Len(t, bag.states, 0)
	comparePageState(t, ps2, *bag.Current())
}

func TestPaginationBagNextPageWithoutActivePageState(t *testing.T) {
	bag := &Bag{}

	// try to unmarshal an empty string
	err := bag.Unmarshal("")
	// passing an empty string SHOULD NOT return error
	require.NoError(t, err)

	updatedToken, err := bag.NextToken("<valid token from third-party service>")
	require.NoError(t, err)
	require.NotEmpty(t, updatedToken)
	require.NotNil(t, bag.currentState)
}

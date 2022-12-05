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

func TestPaginationBag(t *testing.T) {
	bag := &Bag{}
	groupResourceType := "group"
	firstPageToken := "page=1"
	firstPageState := PageState{
		Token:          firstPageToken,
		ResourceTypeID: groupResourceType,
	}
	userResourceType := "user"
	secondPageToken := "page=2"
	secondPageState := PageState{
		Token:          secondPageToken,
		ResourceTypeID: userResourceType,
	}

	bag.push(firstPageState)
	bag.push(secondPageState)
	require.Equal(t, *bag.Current(), secondPageState)
	secondPagePopped := bag.pop()
	require.Equal(t, *secondPagePopped, secondPageState)
	require.Equal(t, *bag.Current(), firstPageState)

	thirdPageToken := "page=3"
	nextPageToken, err := bag.NextToken(thirdPageToken)
	require.NoError(t, err)
	pageToken, err := bag.Marshal()
	require.NoError(t, err)
	require.Equal(t, nextPageToken, pageToken)

	fourthPageToken := "page=4"
	fourthPageState := PageState{
		Token:          fourthPageToken,
		ResourceTypeID: groupResourceType,
	}
	err = bag.Next(fourthPageToken)
	require.NoError(t, err)
	require.Equal(t, *bag.Current(), fourthPageState)
}
